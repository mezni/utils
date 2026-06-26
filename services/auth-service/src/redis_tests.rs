#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test;
    use actix_web::App;
    use bornemap_db::RedisClient;
    use std::time::Duration;

    // Mock Redis client for testing
    struct MockRedisClient {
        data: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, String>>>,
        should_fail: bool,
    }

    impl MockRedisClient {
        fn new() -> Self {
            Self {
                data: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
                should_fail: false,
            }
        }

        fn fail_next(mut self) -> Self {
            self.should_fail = true;
            self
        }
    }

    impl RedisClient for MockRedisClient {
        async fn set_with_ttl(&self, key: &str, value: &str, _ttl_seconds: u64) -> Result<(), bornemap_core::AppError> {
            if self.should_fail {
                return Err(bornemap_core::AppError::InternalError("Mock Redis failure".to_string()));
            }
            let mut data = self.data.lock().unwrap();
            data.insert(key.to_string(), value.to_string());
            Ok(())
        }

        async fn get(&self, key: &str) -> Result<Option<String>, bornemap_core::AppError> {
            if self.should_fail {
                return Err(bornemap_core::AppError::InternalError("Mock Redis failure".to_string()));
            }
            let data = self.data.lock().unwrap();
            Ok(data.get(key).cloned())
        }

        async fn delete(&self, key: &str) -> Result<(), bornemap_core::AppError> {
            if self.should_fail {
                return Err(bornemap_core::AppError::InternalError("Mock Redis failure".to_string()));
            }
            let mut data = self.data.lock().unwrap();
            data.remove(key);
            Ok(())
        }

        async fn exists(&self, key: &str) -> Result<bool, bornemap_core::AppError> {
            if self.should_fail {
                return Err(bornemap_core::AppError::InternalError("Mock Redis failure".to_string()));
            }
            let data = self.data.lock().unwrap();
            Ok(data.contains_key(key))
        }

        async fn increment(&self, key: &str) -> Result<i64, bornemap_core::AppError> {
            if self.should_fail {
                return Err(bornemap_core::AppError::InternalError("Mock Redis failure".to_string()));
            }
            let mut data = self.data.lock().unwrap();
            let value = data.get(key)
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(0);
            let new_value = value + 1;
            data.insert(key.to_string(), new_value.to_string());
            Ok(new_value)
        }

        async fn set_if_not_exists(&self, key: &str, value: &str, _ttl_seconds: u64) -> Result<bool, bornemap_core::AppError> {
            if self.should_fail {
                return Err(bornemap_core::AppError::InternalError("Mock Redis failure".to_string()));
            }
            let mut data = self.data.lock().unwrap();
            if data.contains_key(key) {
                Ok(false)
            } else {
                data.insert(key.to_string(), value.to_string());
                Ok(true)
            }
        }

        async fn ttl(&self, key: &str) -> Result<i64, bornemap_core::AppError> {
            if self.should_fail {
                return Err(bornemap_core::AppError::InternalError("Mock Redis failure".to_string()));
            }
            // Mock: always return -1 (no expiration)
            Ok(-1)
        }

        async fn exists_and_valid(&self, key: &str) -> Result<bool, bornemap_core::AppError> {
            if self.should_fail {
                return Err(bornemap_core::AppError::InternalError("Mock Redis failure".to_string()));
            }
            let data = self.data.lock().unwrap();
            Ok(data.contains_key(key))
        }

        async fn close(&self) -> Result<(), bornemap_core::AppError> {
            if self.should_fail {
                return Err(bornemap_core::AppError::InternalError("Mock Redis failure".to_string()));
            }
            Ok(())
        }

        async fn health_check(&self) -> Result<(), bornemap_core::AppError> {
            if self.should_fail {
                return Err(bornemap_core::AppError::InternalError("Mock Redis failure".to_string()));
            }
            Ok(())
        }
    }

    // Test Redis client initialization
    #[tokio::test]
    async fn test_redis_client_initialization() {
        let redis_client = MockRedisClient::new();
        let result = redis_client.initialize().await;
        assert!(result.is_ok());
    }

    // Test Redis basic operations
    #[tokio::test]
    async fn test_redis_basic_operations() {
        let redis_client = MockRedisClient::new();
        
        // Test set with TTL
        let result = redis_client.set_with_ttl("test_key", "test_value", 300).await;
        assert!(result.is_ok());

        // Test get
        let result = redis_client.get("test_key").await;
        assert_eq!(result.unwrap(), Some("test_value".to_string()));

        // Test exists
        let result = redis_client.exists("test_key").await;
        assert!(result.unwrap());

        // Test delete
        let result = redis_client.delete("test_key").await;
        assert!(result.is_ok());

        // Test get after delete
        let result = redis_client.get("test_key").await;
        assert_eq!(result.unwrap(), None);
    }

    // Test Redis increment operation
    #[tokio::test]
    async fn test_redis_increment() {
        let redis_client = MockRedisClient::new();
        
        let result = redis_client.increment("counter").await;
        assert_eq!(result.unwrap(), 1);

        let result = redis_client.increment("counter").await;
        assert_eq!(result.unwrap(), 2);
    }

    // Test Redis set_if_not_exists
    #[tokio::test]
    async fn test_redis_set_if_not_exists() {
        let redis_client = MockRedisClient::new();
        
        // First call should succeed (key doesn't exist)
        let result = redis_client.set_if_not_exists("new_key", "new_value", 300).await;
        assert!(result.unwrap());

        // Second call should fail (key exists)
        let result = redis_client.set_if_not_exists("new_key", "other_value", 300).await;
        assert!(!result.unwrap());
    }

    // Test Redis error handling
    #[tokio::test]
    async fn test_redis_error_handling() {
        let redis_client = MockRedisClient::new().fail_next();
        
        let result = redis_client.set_with_ttl("test_key", "test_value", 300).await;
        assert!(result.is_err());
    }

    // Test Redis health check
    #[tokio::test]
    async fn test_redis_health_check() {
        let redis_client = MockRedisClient::new();
        
        let result = redis_client.health_check().await;
        assert!(result.is_ok());
    }

    // Test Redis close
    #[tokio::test]
    async fn test_redis_close() {
        let redis_client = MockRedisClient::new();
        
        let result = redis_client.close().await;
        assert!(result.is_ok());
    }

    // Test Redis configuration
    #[tokio::test]
    async fn test_redis_config() {
        let config = redis_config::RedisConfig::new(
            "redis://localhost:6379".to_string(),
            100,
            60,
            300,
            3,
            100,
        );

        assert_eq!(config.redis_url, "redis://localhost:6379");
        assert_eq!(config.rate_limit_requests, 100);
        assert_eq!(config.rate_limit_window_seconds, 60);
        assert_eq!(config.oauth_state_ttl_seconds, 300);

        // Test TTL durations
        let oauth_ttl = config.get_oauth_state_ttl();
        assert_eq!(oauth_ttl.as_secs(), 300);

        let rate_limit_window = config.get_rate_limit_window();
        assert_eq!(rate_limit_window.as_secs(), 60);
    }

    // Test Redis configuration validation
    #[tokio::test]
    async fn test_redis_config_validation() {
        let mut config = redis_config::RedisConfig::new(
            "redis://localhost:6379".to_string(),
            100,
            60,
            300,
            3,
            100,
        );

        // Valid configuration
        assert!(config.validate().is_ok());

        // Invalid rate limit requests
        config.rate_limit_requests = 0;
        assert!(config.validate().is_err());

        // Invalid rate limit window
        config.rate_limit_requests = 100;
        config.rate_limit_window_seconds = 0;
        assert!(config.validate().is_err());

        // Invalid OAuth state TTL
        config.rate_limit_window_seconds = 60;
        config.oauth_state_ttl_seconds = 0;
        assert!(config.validate().is_err());

        // Empty Redis URL
        config.oauth_state_ttl_seconds = 300;
        config.redis_url = "".to_string();
        assert!(config.validate().is_err());
    }

    // Test OAuth state store with mock Redis
    #[tokio::test]
    async fn test_oauth_state_store_with_mock() {
        let redis_client = MockRedisClient::new();
        let state_store = application::oauth_state::RedisOAuthStateStore::new_with_default(redis_client)
            .await
            .unwrap();

        let state = application::oauth_state::RedisOAuthStateStore::generate_state();
        
        // Test storing state
        let result = state_store.store_oauth_state(&state).await;
        assert!(result.is_ok());

        // Test validating state (should succeed)
        let result = state_store.validate_oauth_state(&state).await;
        assert!(result.is_ok());

        // Test validating same state again (should fail)
        let result = state_store.validate_oauth_state(&state).await;
        assert!(result.is_err());
    }

    // Test OAuth state store mock implementation
    #[tokio::test]
    async fn test_oauth_state_store_mock() {
        let state_store = application::oauth_state::MockOAuthStateStore::new();
        let state = "test-state-123";

        // Test storing state
        let result = state_store.store_oauth_state(state).await;
        assert!(result.is_ok());

        // Test validating state (should succeed)
        let result = state_store.validate_oauth_state(state).await;
        assert!(result.is_ok());

        // Test validating same state again (should fail)
        let result = state_store.validate_oauth_state(state).await;
        assert!(result.is_err());
    }

    // Test rate limiting middleware with mock
    #[test]
    fn test_rate_limit_config_from_env() {
        // Set environment variables
        std::env::set_var("RATE_LIMIT_REQUESTS", "50");
        std::env::set_var("RATE_LIMIT_WINDOW_SECONDS", "30");

        let config = http::middleware::RateLimitConfig::from_env();
        assert!(config.is_ok());

        let config = config.unwrap();
        assert_eq!(config.requests_per_window, 50);
        assert_eq!(config.window_seconds, 30);
    }

    // Test endpoint rate limits
    #[test]
    fn test_endpoint_rate_limits() {
        let limits = http::middleware::EndpointRateLimits::default();
        
        assert_eq!(limits.register.requests_per_window, 5);
        assert_eq!(limits.register.window_seconds, 300);
        assert_eq!(limits.login.requests_per_window, 10);
        assert_eq!(limits.login.window_seconds, 300);
        assert_eq!(limits.oauth_start.requests_per_window, 20);
        assert_eq!(limits.oauth_start.window_seconds, 60);
        assert_eq!(limits.oauth_callback.requests_per_window, 30);
        assert_eq!(limits.oauth_callback.window_seconds, 60);
    }

    // Test Redis session helper with mock
    #[tokio::test]
    async fn test_session_helper_with_mock() {
        let redis_client = MockRedisClient::new();
        let session_helper = infrastructure::redis::RedisSessionHelper::new(redis_client);

        // Test basic operations
        session_helper.store("test_key", "test_value").await.unwrap();
        let value = session_helper.get("test_key").await.unwrap();
        assert_eq!(value, Some("test_value".to_string()));

        session_helper.delete("test_key").await.unwrap();
        let value = session_helper.get("test_key").await.unwrap();
        assert_eq!(value, None);
    }

    // Test Redis session helper with TTL
    #[tokio::test]
    async fn test_session_helper_with_ttl() {
        let redis_client = MockRedisClient::new();
        let session_helper = infrastructure::redis::RedisSessionHelper::new(redis_client);

        session_helper.store_with_ttl("ttl_key", "ttl_value", Duration::from_secs(300)).await.unwrap();
        let value = session_helper.get("ttl_key").await.unwrap();
        assert_eq!(value, Some("ttl_value".to_string()));
    }

    // Test Redis session helper email verification
    #[tokio::test]
    async fn test_session_helper_email_verification() {
        let redis_client = MockRedisClient::new();
        let session_helper = infrastructure::redis::RedisSessionHelper::new(redis_client);
        let email = "test@example.com";
        let token = "verification-token-123";

        session_helper.store_email_verification(email, token).await.unwrap();
        let stored_token = session_helper.get_email_verification(email).await.unwrap();
        assert_eq!(stored_token, Some(token.to_string()));
    }

    // Test Redis session helper password reset
    #[tokio::test]
    async fn test_session_helper_password_reset() {
        let redis_client = MockRedisClient::new();
        let session_helper = infrastructure::redis::RedisSessionHelper::new(redis_client);
        let email = "test@example.com";
        let token = "reset-token-123";

        session_helper.store_password_reset(email, token).await.unwrap();
        let stored_token = session_helper.get_password_reset(email).await.unwrap();
        assert_eq!(stored_token, Some(token.to_string()));
    }

    // Test Redis session helper MFA challenge
    #[tokio::test]
    async fn test_session_helper_mfa_challenge() {
        let redis_client = MockRedisClient::new();
        let session_helper = infrastructure::redis::RedisSessionHelper::new(redis_client);
        let user_id = "user-123";
        let challenge = "mfa-challenge-123";

        session_helper.store_mfa_challenge(user_id, challenge).await.unwrap();
        let stored_challenge = session_helper.get_mfa_challenge(user_id).await.unwrap();
        assert_eq!(stored_challenge, Some(challenge.to_string()));
    }

    // Test Redis session helper login attempt tracking
    #[tokio::test]
    async fn test_session_helper_login_attempt_tracking() {
        let redis_client = MockRedisClient::new();
        let session_helper = infrastructure::redis::RedisSessionHelper::new(redis_client);
        let ip = "192.168.1.1";

        let count1 = session_helper.track_login_attempt(ip).await.unwrap();
        assert_eq!(count1, 1);

        let count2 = session_helper.track_login_attempt(ip).await.unwrap();
        assert_eq!(count2, 2);

        session_helper.reset_login_attempts(ip).await.unwrap();
        let count3 = session_helper.track_login_attempt(ip).await.unwrap();
        assert_eq!(count3, 1);
    }

    // Test Redis session helper temp token
    #[tokio::test]
    async fn test_session_helper_temp_token() {
        let redis_client = MockRedisClient::new();
        let session_helper = infrastructure::redis::RedisSessionHelper::new(redis_client);
        let token = "temp-token-123";
        let user_id = "user-123";

        session_helper.store_temp_token(token, user_id).await.unwrap();
        let stored_user_id = session_helper.get_temp_token_user(token).await.unwrap();
        assert_eq!(stored_user_id, Some(user_id.to_string()));
    }
}