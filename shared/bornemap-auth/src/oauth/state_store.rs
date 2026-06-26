use async_trait::async_trait;
use bornemap_core::AppError;
use redis::AsyncCommands;
use std::time::Duration;
use tokio::sync::OnceCell;

pub struct RedisOAuthStateStore {
    client: OnceCell<redis::Client>,
}

impl RedisOAuthStateStore {
    pub fn new(redis_url: &str) -> Result<Self, AppError> {
        let client = redis::Client::open(redis_url)
            .map_err(|e| AppError::OAuthStateStoreError(e.to_string()))?;
        
        Ok(Self {
            client: OnceCell::new_with(client),
        })
    }

    async fn get_client(&self) -> Result<&redis::Client, AppError> {
        self.client
            .get()
            .ok_or_else(|| AppError::OAuthStateStoreError("Redis client not initialized".to_string()))
    }
}

#[async_trait]
pub trait OAuthStateStore: Send + Sync {
    async fn create(&self, state: &str, ttl: Duration) -> Result<(), AppError>;

    async fn consume(&self, state: &str) -> Result<bool, AppError>;
}

#[async_trait]
impl OAuthStateStore for RedisOAuthStateStore {
    async fn create(&self, state: &str, ttl: Duration) -> Result<(), AppError> {
        let mut conn = self.get_client()
            .await?
            .get_async_connection()
            .await
            .map_err(|e| AppError::OAuthStateStoreError(e.to_string()))?;

        let key = format("oauth_state:{}", state);
        let _: () = conn
            .set_ex(key, true, ttl.as_secs() as i64)
            .await
            .map_err(|e| AppError::OAuthStateStoreError(e.to_string()))?;

        Ok(())
    }

    async fn consume(&self, state: &str) -> Result<bool, AppError> {
        let mut conn = self.get_client()
            .await?
            .get_async_connection()
            .await
            .map_err(|e| AppError::OAuthStateStoreError(e.to_string()))?;

        let key = format("oauth_state:{}", state);
        
        // Use Redis WATCH/MULTI/EXEC pattern for atomic consume operation
        let mut pipe = redis::pipe();
        pipe.atomic()
            .get(&key)
            .del(&key);

        let result: Option<(Option<String>, i64)> = pipe
            .query_async(&mut conn)
            .await
            .map_err(|e| AppError::OAuthStateStoreError(e.to_string()))?;

        match result {
            Some((Some(_), _)) => Ok(true), // State existed and was consumed
            Some((None, _)) => Ok(false), // State didn't exist (already consumed)
            None => Ok(false), // Key didn't exist
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // Mock implementation for testing
    struct MockOAuthStateStore {
        states: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    }

    impl MockOAuthStateStore {
        fn new() -> Self {
            Self {
                states: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
            }
        }
    }

    #[async_trait]
    impl OAuthStateStore for MockOAuthStateStore {
        async fn create(&self, state: &str, _ttl: Duration) -> Result<(), AppError> {
            let mut states = self.states.lock().unwrap();
            states.insert(state.to_string());
            Ok(())
        }

        async fn consume(&self, state: &str) -> Result<bool, AppError> {
            let mut states = self.states.lock().unwrap();
            let existed = states.remove(state);
            Ok(existed)
        }
    }

    #[tokio::test]
    async fn test_state_creation_and_consumption() {
        let store = MockOAuthStateStore::new();
        let state = "test-state-123";

        // Create state
        store.create(state, Duration::from_secs(300)).await.unwrap();
        
        // Consume state (should succeed)
        let result = store.consume(state).await.unwrap();
        assert!(result);

        // Try to consume again (should fail)
        let result = store.consume(state).await.unwrap();
        assert!(!result);
    }

    #[tokio::test]
    async fn test_consume_nonexistent_state() {
        let store = MockOAuthStateStore::new();
        let result = store.consume("nonexistent-state").await.unwrap();
        assert!(!result);
    }
}