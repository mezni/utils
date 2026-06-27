use std::time::Duration;
use bornemap_db::{RedisClient, RedisKeys};
use bornemap_core::AppError;

pub struct RedisSessionHelper {
    redis_client: RedisClient,
}

impl RedisSessionHelper {
    pub fn new(redis_client: RedisClient) -> Self {
        Self { redis_client }
    }

    pub async fn store_with_ttl(&self, key: &str, value: &str, ttl: Duration) -> Result<(), AppError> {
        let redis_key = RedisKeys::generic_key("session", key);
        self.redis_client
            .set_with_ttl(&redis_key, value, ttl.as_secs())
            .await
            .map_err(|_| AppError::InternalError)?;
        Ok(())
    }

    pub async fn store(&self, key: &str, value: &str) -> Result<(), AppError> {
        self.store_with_ttl(key, value, Duration::from_secs(3600)).await
    }

    pub async fn get(&self, key: &str) -> Result<Option<String>, AppError> {
        let redis_key = RedisKeys::generic_key("session", key);
        self.redis_client
            .get(&redis_key)
            .await
            .map_err(|_| AppError::InternalError)
    }

    pub async fn delete(&self, key: &str) -> Result<(), AppError> {
        let redis_key = RedisKeys::generic_key("session", key);
        self.redis_client
            .delete(&redis_key)
            .await
            .map_err(|_| AppError::InternalError)?;
        Ok(())
    }

    pub async fn exists(&self, key: &str) -> Result<bool, AppError> {
        let redis_key = RedisKeys::generic_key("session", key);
        self.redis_client
            .exists(&redis_key)
            .await
            .map_err(|_| AppError::InternalError)
    }

    pub async fn store_email_verification(&self, email: &str, token: &str) -> Result<(), AppError> {
        let key = RedisKeys::email_verification(email);
        let ttl = RedisKeys::email_verification_ttl();
        self.redis_client
            .set_with_ttl(&key, token, ttl.as_secs())
            .await
            .map_err(|_| AppError::InternalError)?;
        Ok(())
    }

    pub async fn get_email_verification(&self, email: &str) -> Result<Option<String>, AppError> {
        let key = RedisKeys::email_verification(email);
        self.redis_client
            .get(&key)
            .await
            .map_err(|_| AppError::InternalError)
    }

    pub async fn store_password_reset(&self, email: &str, token: &str) -> Result<(), AppError> {
        let key = RedisKeys::password_reset(email);
        let ttl = RedisKeys::password_reset_ttl();
        self.redis_client
            .set_with_ttl(&key, token, ttl.as_secs())
            .await
            .map_err(|_| AppError::InternalError)?;
        Ok(())
    }

    pub async fn get_password_reset(&self, email: &str) -> Result<Option<String>, AppError> {
        let key = RedisKeys::password_reset(email);
        self.redis_client
            .get(&key)
            .await
            .map_err(|_| AppError::InternalError)
    }

    pub async fn store_mfa_challenge(&self, user_id: &str, challenge: &str) -> Result<(), AppError> {
        let key = RedisKeys::mfa_challenge(user_id);
        let ttl = RedisKeys::mfa_challenge_ttl();
        self.redis_client
            .set_with_ttl(&key, challenge, ttl.as_secs())
            .await
            .map_err(|_| AppError::InternalError)?;
        Ok(())
    }

    pub async fn get_mfa_challenge(&self, user_id: &str) -> Result<Option<String>, AppError> {
        let key = RedisKeys::mfa_challenge(user_id);
        self.redis_client
            .get(&key)
            .await
            .map_err(|_| AppError::InternalError)
    }

    pub async fn track_login_attempt(&self, ip: &str) -> Result<i64, AppError> {
        let key = RedisKeys::login_attempts(ip);
        let ttl = RedisKeys::login_attempts_ttl();

        let exists = self.redis_client.exists(&key).await.map_err(|_| AppError::InternalError)?;
        if !exists {
            self.redis_client.set_with_ttl(&key, "0", ttl.as_secs()).await.map_err(|_| AppError::InternalError)?;
        }

        let count = self.redis_client.increment(&key).await.map_err(|_| AppError::InternalError)?;
        Ok(count as i64)
    }

    pub async fn reset_login_attempts(&self, ip: &str) -> Result<(), AppError> {
        let key = RedisKeys::login_attempts(ip);
        self.redis_client.delete(&key).await.map_err(|_| AppError::InternalError)?;
        Ok(())
    }

    pub async fn store_temp_token(&self, token: &str, user_id: &str) -> Result<(), AppError> {
        let key = RedisKeys::temp_token(token);
        let ttl = RedisKeys::temp_token_ttl();
        self.redis_client
            .set_with_ttl(&key, user_id, ttl.as_secs())
            .await
            .map_err(|_| AppError::InternalError)?;
        Ok(())
    }

    pub async fn get_temp_token_user(&self, token: &str) -> Result<Option<String>, AppError> {
        let key = RedisKeys::temp_token(token);
        self.redis_client
            .get(&key)
            .await
            .map_err(|_| AppError::InternalError)
    }

    pub async fn cleanup_expired(&self) -> Result<u64, AppError> {
        Ok(0)
    }
}


