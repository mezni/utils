use async_trait::async_trait;
use bornemap_core::AppError;
use bornemap_db::{RedisClient, RedisKeys};
use std::sync::Arc;
use std::time::Duration;

#[async_trait]
pub trait OAuthStateStore: Send + Sync {
    async fn store_oauth_state(&self, state: &str) -> Result<(), AppError>;
    async fn validate_oauth_state(&self, state: &str) -> Result<bool, AppError>;
    async fn create(&self, state: &str, ttl: Duration) -> Result<(), AppError>;
    async fn consume(&self, state: &str) -> Result<bool, AppError>;
}

pub struct RedisOAuthStateStore {
    redis_client: Arc<RedisClient>,
}

impl RedisOAuthStateStore {
    pub fn new(redis_client: Arc<RedisClient>) -> Self {
        Self { redis_client }
    }

    pub fn generate_state() -> String {
        uuid::Uuid::new_v4().to_string()
    }
}

#[async_trait]
impl OAuthStateStore for RedisOAuthStateStore {
    async fn store_oauth_state(&self, state: &str) -> Result<(), AppError> {
        let key = RedisKeys::oauth_state(state);
        let ttl = RedisKeys::oauth_state_ttl();
        self.redis_client
            .set_with_ttl(&key, state, ttl.as_secs())
            .await
            .map_err(|_| AppError::OAuthStateStoreError("Failed to store OAuth state".to_string()))
    }

    async fn validate_oauth_state(&self, state: &str) -> Result<bool, AppError> {
        self.consume(state).await
    }

    async fn create(&self, state: &str, ttl: Duration) -> Result<(), AppError> {
        let key = RedisKeys::oauth_state(state);
        self.redis_client
            .set_with_ttl(&key, state, ttl.as_secs())
            .await
            .map_err(|_| AppError::OAuthStateStoreError("Failed to create OAuth state".to_string()))
    }

    async fn consume(&self, state: &str) -> Result<bool, AppError> {
        let key = RedisKeys::oauth_state(state);
        let exists = self.redis_client
            .exists(&key)
            .await
            .map_err(|_| AppError::OAuthStateStoreError("Failed to check OAuth state".to_string()))?;

        if exists {
            self.redis_client
                .delete(&key)
                .await
                .map_err(|_| AppError::OAuthStateStoreError("Failed to delete OAuth state".to_string()))?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
