use async_trait::async_trait;
use bornemap_core::AppError;
use bornemap_db::RedisClient;
use std::time::Duration;

#[async_trait]
pub trait OAuthStateStore: Send + Sync {
    async fn store_oauth_state(&self, state: &str) -> Result<(), AppError>;
    async fn validate_oauth_state(&self, state: &str) -> Result<(), AppError>;
}

/// Redis-based OAuth state store implementation
pub struct RedisOAuthStateStore {
    redis_client: RedisClient,
    ttl_seconds: i64,
}

impl RedisOAuthStateStore {
    pub fn new(redis_client: RedisClient, ttl_seconds: i64) -> Self {
        Self {
            redis_client,
            ttl_seconds,
        }
    }

    pub fn with_default_ttl(redis_client: RedisClient) -> Self {
        Self {
            redis_client,
            ttl_seconds: 300, // 5 minutes default
        }
    }

    /// Generate a secure random state for OAuth anti-CSRF protection
    pub fn generate_state() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    /// Create a new OAuth state store with default configuration
    pub async fn new_with_default(redis_client: RedisClient) -> Result<Self, AppError> {
        let store = Self::with_default_ttl(redis_client);
        store.initialize().await?;
        Ok(store)
    }

    /// Initialize the Redis connection
    async fn initialize(&self) -> Result<(), AppError> {
        self.redis_client.initialize()
            .await
            .map_err(|e| AppError::ConfigurationError(format!("Failed to initialize Redis connection: {}", e)))
    }
}

#[async_trait]
impl OAuthStateStore for RedisOAuthStateStore {
    async fn store_oauth_state(&self, state: &str) -> Result<(), AppError> {
        let key = bornemap_db::RedisKeys::oauth_state(state);
        let ttl = Duration::from_secs(self.ttl_seconds as u64);
        
        self.redis_client
            .set_with_ttl(&key, "1", ttl.as_secs())
            .await
            .map_err(|e| AppError::InternalError(format!("Failed to store OAuth state: {}", e)))?;

        Ok(())
    }

    async fn validate_oauth_state(&self, state: &str) -> Result<(), AppError> {
        let key = bornemap_db::RedisKeys::oauth_state(state);
        
        // Check if state exists and is not expired
        let exists = self.redis_client
            .exists_and_valid(&key)
            .await
            .map_err(|e| AppError::InternalError(format!("Failed to validate OAuth state: {}", e)))?;

        if !exists {
            return Err(AppError::InvalidSession("Invalid or expired OAuth state".to_string()));
        }

        // Delete the state immediately after successful validation (one-time use)
        self.redis_client
            .delete(&key)
            .await
            .map_err(|e| AppError::InternalError(format!("Failed to delete OAuth state: {}", e)))?;

        Ok(())
    }
}

/// Mock OAuth state store for testing
pub struct MockOAuthStateStore {
    states: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
}

impl MockOAuthStateStore {
    pub fn new() -> Self {
        Self {
            states: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        }
    }

    pub fn with_states(states: Vec<String>) -> Self {
        let state_set: std::collections::HashSet<String> = states.into_iter().collect();
        Self {
            states: std::sync::Arc::new(std::sync::Mutex::new(state_set)),
        }
    }
}

#[async_trait]
impl OAuthStateStore for MockOAuthStateStore {
    async fn store_oauth_state(&self, state: &str) -> Result<(), AppError> {
        let mut states = self.states.lock().unwrap();
        states.insert(state.to_string());
        Ok(())
    }

    async fn validate_oauth_state(&self, state: &str) -> Result<(), AppError> {
        let mut states = self.states.lock().unwrap();
        let existed = states.remove(state);
        
        if !existed {
            return Err(AppError::InvalidSession("Invalid or expired OAuth state".to_string()));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_test;

    #[tokio::test]
    async fn test_redis_oauth_state_store() {
        // This test would require a real Redis instance
        // For now, we'll test the mock implementation
        let store = MockOAuthStateStore::new();
        let state = "test-state-123";

        // Test storing state
        let result = store.store_oauth_state(state).await;
        assert!(result.is_ok());

        // Test validating state (should succeed)
        let result = store.validate_oauth_state(state).await;
        assert!(result.is_ok());

        // Test validating same state again (should fail)
        let result = store.validate_oauth_state(state).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(AppError::InvalidSession(_))));
    }

    #[tokio::test]
    async fn test_redis_oauth_state_store_invalid_state() {
        let store = MockOAuthStateStore::new();
        let state = "invalid-state";

        // Test validating non-existent state (should fail)
        let result = store.validate_oauth_state(state).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(AppError::InvalidSession(_))));
    }

    #[tokio::test]
    async fn test_redis_oauth_state_store_multiple_states() {
        let store = MockOAuthStateStore::new();
        let state1 = "state-1";
        let state2 = "state-2";

        // Store multiple states
        store.store_oauth_state(state1).await.unwrap();
        store.store_oauth_state(state2).await.unwrap();

        // Validate first state
        let result = store.validate_oauth_state(state1).await;
        assert!(result.is_ok());

        // Validate second state
        let result = store.validate_oauth_state(state2).await;
        assert!(result.is_ok());

        // Both should now be invalid
        let result1 = store.validate_oauth_state(state1).await;
        let result2 = store.validate_oauth_state(state2).await;
        assert!(result1.is_err());
        assert!(result2.is_err());
    }

    #[test]
    fn test_generate_state() {
        let state1 = RedisOAuthStateStore::generate_state();
        let state2 = RedisOAuthStateStore::generate_state();

        // States should be unique
        assert_ne!(state1, state2);
        
        // States should be valid UUIDs
        assert_eq!(state1.len(), 36); // UUID length
        assert_eq!(state2.len(), 36);
    }
}