use std::time::Duration;
use bornemap_db::{RedisClient, RedisKeys};
use bornemap_core::AppError;

/// Redis session helper for temporary authentication data
pub struct RedisSessionHelper {
    redis_client: RedisClient,
}

impl RedisSessionHelper {
    /// Create a new Redis session helper
    pub fn new(redis_client: RedisClient) -> Self {
        Self { redis_client }
    }

    /// Store a value with TTL
    pub async fn store_with_ttl(&self, key: &str, value: &str, ttl: Duration) -> Result<(), AppError> {
        let redis_key = RedisKeys::generic_key("session", key);
        self.redis_client
            .set_with_ttl(&redis_key, value, ttl.as_secs())
            .await
            .map_err(|e| AppError::InternalError(format!("Failed to store session data: {}", e)))?;
        
        Ok(())
    }

    /// Store a value with default TTL (1 hour)
    pub async fn store(&self, key: &str, value: &str) -> Result<(), AppError> {
        self.store_with_ttl(key, value, Duration::from_secs(3600)).await
    }

    /// Get a value by key
    pub async fn get(&self, key: &str) -> Result<Option<String>, AppError> {
        let redis_key = RedisKeys::generic_key("session", key);
        self.redis_client
            .get(&redis_key)
            .await
            .map_err(|e| AppError::InternalError(format!("Failed to get session data: {}", e)))?
    }

    /// Delete a value by key
    pub async fn delete(&self, key: &str) -> Result<(), AppError> {
        let redis_key = RedisKeys::generic_key("session", key);
        self.redis_client
            .delete(&redis_key)
            .await
            .map_err(|e| AppError::InternalError(format!("Failed to delete session data: {}", e)))?;
        
        Ok(())
    }

    /// Check if a key exists and is not expired
    pub async fn exists(&self, key: &str) -> Result<bool, AppError> {
        let redis_key = RedisKeys::generic_key("session", key);
        self.redis_client
            .exists_and_valid(&redis_key)
            .await
            .map_err(|e| AppError::InternalError(format!("Failed to check session data existence: {}", e)))?
    }

    /// Store email verification data
    pub async fn store_email_verification(&self, email: &str, token: &str) -> Result<(), AppError> {
        let key = RedisKeys::email_verification(email);
        let ttl = RedisKeys::email_verification_ttl();
        self.redis_client
            .set_with_ttl(&key, token, ttl.as_secs())
            .await
            .map_err(|e| AppError::InternalError(format!("Failed to store email verification data: {}", e)))?;
        
        Ok(())
    }

    /// Get email verification token
    pub async fn get_email_verification(&self, email: &str) -> Result<Option<String>, AppError> {
        let key = RedisKeys::email_verification(email);
        self.redis_client
            .get(&key)
            .await
            .map_err(|e| AppError::InternalError(format!("Failed to get email verification data: {}", e)))?
    }

    /// Store password reset data
    pub async fn store_password_reset(&self, email: &str, token: &str) -> Result<(), AppError> {
        let key = RedisKeys::password_reset(email);
        let ttl = RedisKeys::password_reset_ttl();
        self.redis_client
            .set_with_ttl(&key, token, ttl.as_secs())
            .await
            .map_err(|e| AppError::InternalError(format!("Failed to store password reset data: {}", e)))?;
        
        Ok(())
    }

    /// Get password reset token
    pub async fn get_password_reset(&self, email: &str) -> Result<Option<String>, AppError> {
        let key = RedisKeys::password_reset(email);
        self.redis_client
            .get(&key)
            .await
            .map_err(|e| AppError::InternalError(format!("Failed to get password reset data: {}", e)))?
    }

    /// Store MFA challenge data
    pub async fn store_mfa_challenge(&self, user_id: &str, challenge: &str) -> Result<(), AppError> {
        let key = RedisKeys::mfa_challenge(user_id);
        let ttl = RedisKeys::mfa_challenge_ttl();
        self.redis_client
            .set_with_ttl(&key, challenge, ttl.as_secs())
            .await
            .map_err(|e| AppError::InternalError(format!("Failed to store MFA challenge data: {}", e)))?;
        
        Ok(())
    }

    /// Get MFA challenge data
    pub async fn get_mfa_challenge(&self, user_id: &str) -> Result<Option<String>, AppError> {
        let key = RedisKeys::mfa_challenge(user_id);
        self.redis_client
            .get(&key)
            .await
            .map_err(|e| AppError::InternalError(format!("Failed to get MFA challenge data: {}", e)))?
    }

    /// Track login attempts for rate limiting
    pub async fn track_login_attempt(&self, ip: &str) -> Result<i64, AppError> {
        let key = RedisKeys::login_attempts(ip);
        let ttl = RedisKeys::login_attempts_ttl();
        
        // Initialize counter if it doesn't exist
        let exists = self.redis_client.exists(&key).await?;
        if !exists {
            self.redis_client.set_with_ttl(&key, "0", ttl.as_secs()).await?;
        }
        
        // Increment and return new count
        let count = self.redis_client.increment(&key).await?;
        Ok(count)
    }

    /// Reset login attempts for an IP
    pub async fn reset_login_attempts(&self, ip: &str) -> Result<(), AppError> {
        let key = RedisKeys::login_attempts(ip);
        self.redis_client.delete(&key).await?;
        Ok(())
    }

    /// Store temporary authentication token
    pub async fn store_temp_token(&self, token: &str, user_id: &str) -> Result<(), AppError> {
        let key = RedisKeys::temp_token(token);
        let ttl = RedisKeys::temp_token_ttl();
        self.redis_client
            .set_with_ttl(&key, user_id, ttl.as_secs())
            .await
            .map_err(|e| AppError::InternalError(format!("Failed to store temporary token: {}", e)))?;
        
        Ok(())
    }

    /// Get user ID from temporary token
    pub async fn get_temp_token_user(&self, token: &str) -> Result<Option<String>, AppError> {
        let key = RedisKeys::temp_token(token);
        self.redis_client
            .get(&key)
            .await
            .map_err(|e| AppError::InternalError(format!("Failed to get temporary token: {}", e)))?
    }

    /// Clean up expired session data (maintenance function)
    pub async fn cleanup_expired(&self) -> Result<u64, AppError> {
        // This is a simplified implementation
        // In production, you might want to use Redis's KEYS command with a pattern
        // or use Redis's built-in TTL expiration
        
        // For now, just return 0 as this would require more complex implementation
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio_test;

    // Mock Redis client for testing
    struct MockRedisClient {
        data: Arc<std::sync::Mutex<std::collections::HashMap<String, String>>>,
    }

    impl MockRedisClient {
        fn new() -> Self {
            Self {
                data: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            }
        }
    }

    impl bornemap_db::RedisClient for MockRedisClient {
        async fn set_with_ttl(&self, key: &str, value: &str, _ttl_seconds: u64) -> Result<(), bornemap_core::AppError> {
            let mut data = self.data.lock().unwrap();
            data.insert(key.to_string(), value.to_string());
            Ok(())
        }

        async fn get(&self, key: &str) -> Result<Option<String>, bornemap_core::AppError> {
            let data = self.data.lock().unwrap();
            Ok(data.get(key).cloned())
        }

        async fn delete(&self, key: &str) -> Result<(), bornemap_core::AppError> {
            let mut data = self.data.lock().unwrap();
            data.remove(key);
            Ok(())
        }

        async fn exists(&self, key: &str) -> Result<bool, bornemap_core::AppError> {
            let data = self.data.lock().unwrap();
            Ok(data.contains_key(key))
        }

        async fn increment(&self, key: &str) -> Result<i64, bornemap_core::AppError> {
            let mut data = self.data.lock().unwrap();
            let value = data.get(key)
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(0);
            let new_value = value + 1;
            data.insert(key.to_string(), new_value.to_string());
            Ok(new_value)
        }

        async fn set_if_not_exists(&self, key: &str, value: &str, _ttl_seconds: u64) -> Result<bool, bornemap_core::AppError> {
            let mut data = self.data.lock().unwrap();
            if data.contains_key(key) {
                Ok(false)
            } else {
                data.insert(key.to_string(), value.to_string());
                Ok(true)
            }
        }

        async fn ttl(&self, key: &str) -> Result<i64, bornemap_core::AppError> {
            // Mock: always return -1 (no expiration)
            Ok(-1)
        }

        async fn exists_and_valid(&self, key: &str) -> Result<bool, bornemap_core::AppError> {
            let data = self.data.lock().unwrap();
            Ok(data.contains_key(key))
        }

        async fn close(&self) -> Result<(), bornemap_core::AppError> {
            Ok(())
        }

        async fn health_check(&self) -> Result<(), bornemap_core::AppError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_session_helper_basic_operations() {
        let redis_client = MockRedisClient::new();
        let session_helper = RedisSessionHelper::new(redis_client);

        // Test store and get
        session_helper.store("test_key", "test_value").await.unwrap();
        let value = session_helper.get("test_key").await.unwrap();
        assert_eq!(value, Some("test_value".to_string()));

        // Test delete
        session_helper.delete("test_key").await.unwrap();
        let value = session_helper.get("test_key").await.unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_session_helper_with_ttl() {
        let redis_client = MockRedisClient::new();
        let session_helper = RedisSessionHelper::new(redis_client);

        // Test store with TTL
        session_helper.store_with_ttl("ttl_key", "ttl_value", Duration::from_secs(300)).await.unwrap();
        let value = session_helper.get("ttl_key").await.unwrap();
        assert_eq!(value, Some("ttl_value".to_string()));
    }

    #[tokio::test]
    async fn test_session_helper_exists() {
        let redis_client = MockRedisClient::new();
        let session_helper = RedisSessionHelper::new(redis_client);

        // Test non-existent key
        let exists = session_helper.exists("nonexistent").await.unwrap();
        assert!(!exists);

        // Test existing key
        session_helper.store("exists_key", "exists_value").await.unwrap();
        let exists = session_helper.exists("exists_key").await.unwrap();
        assert!(exists);
    }

    #[tokio::test]
    async fn test_email_verification_operations() {
        let redis_client = MockRedisClient::new();
        let session_helper = RedisSessionHelper::new(redis_client);
        let email = "test@example.com";
        let token = "verification-token-123";

        // Store and get email verification
        session_helper.store_email_verification(email, token).await.unwrap();
        let stored_token = session_helper.get_email_verification(email).await.unwrap();
        assert_eq!(stored_token, Some(token.to_string()));
    }

    #[tokio::test]
    async fn test_password_reset_operations() {
        let redis_client = MockRedisClient::new();
        let session_helper = RedisSessionHelper::new(redis_client);
        let email = "test@example.com";
        let token = "reset-token-123";

        // Store and get password reset
        session_helper.store_password_reset(email, token).await.unwrap();
        let stored_token = session_helper.get_password_reset(email).await.unwrap();
        assert_eq!(stored_token, Some(token.to_string()));
    }

    #[tokio::test]
    async fn test_mfa_challenge_operations() {
        let redis_client = MockRedisClient::new();
        let session_helper = RedisSessionHelper::new(redis_client);
        let user_id = "user-123";
        let challenge = "mfa-challenge-123";

        // Store and get MFA challenge
        session_helper.store_mfa_challenge(user_id, challenge).await.unwrap();
        let stored_challenge = session_helper.get_mfa_challenge(user_id).await.unwrap();
        assert_eq!(stored_challenge, Some(challenge.to_string()));
    }

    #[tokio::test]
    async fn test_login_attempt_tracking() {
        let redis_client = MockRedisClient::new();
        let session_helper = RedisSessionHelper::new(redis_client);
        let ip = "192.168.1.1";

        // Track multiple login attempts
        let count1 = session_helper.track_login_attempt(ip).await.unwrap();
        assert_eq!(count1, 1);
        
        let count2 = session_helper.track_login_attempt(ip).await.unwrap();
        assert_eq!(count2, 2);

        // Reset attempts
        session_helper.reset_login_attempts(ip).await.unwrap();
        let count3 = session_helper.track_login_attempt(ip).await.unwrap();
        assert_eq!(count3, 1);
    }

    #[tokio::test]
    async fn test_temp_token_operations() {
        let redis_client = MockRedisClient::new();
        let session_helper = RedisSessionHelper::new(redis_client);
        let token = "temp-token-123";
        let user_id = "user-123";

        // Store and get temp token
        session_helper.store_temp_token(token, user_id).await.unwrap();
        let stored_user_id = session_helper.get_temp_token_user(token).await.unwrap();
        assert_eq!(stored_user_id, Some(user_id.to_string()));
    }
}