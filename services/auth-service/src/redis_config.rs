use std::time::Duration;
use bornemap_db::RedisClient;
use bornemap_core::AppError;

#[derive(Clone)]
pub struct RedisConfig {
    pub redis_url: String,
    pub rate_limit_requests: u32,
    pub rate_limit_window_seconds: u64,
    pub oauth_state_ttl_seconds: i64,
    pub max_retries: u32,
    pub retry_delay_ms: u64,
}

impl RedisConfig {
    pub fn from_env() -> Result<Self, AppError> {
        let redis_url = std::env::var("REDIS_URL")
            .map_err(|_| AppError::ConfigurationError("REDIS_URL required".into()))?
            .to_string();

        let rate_limit_requests: u32 = std::env::var("RATE_LIMIT_REQUESTS")
            .unwrap_or_else(|_| "100".into())
            .parse()
            .map_err(|_| AppError::ConfigurationError("Invalid RATE_LIMIT_REQUESTS".into()))?;

        let rate_limit_window_seconds: u64 = std::env::var("RATE_LIMIT_WINDOW_SECONDS")
            .unwrap_or_else(|_| "60".into())
            .parse()
            .map_err(|_| AppError::ConfigurationError("Invalid RATE_LIMIT_WINDOW_SECONDS".into()))?;

        let oauth_state_ttl_seconds: i64 = std::env::var("OAUTH_STATE_TTL")
            .unwrap_or_else(|_| "300".into())
            .parse()
            .map_err(|_| AppError::ConfigurationError("Invalid OAUTH_STATE_TTL".into()))?;

        Ok(Self {
            redis_url,
            rate_limit_requests,
            rate_limit_window_seconds,
            oauth_state_ttl_seconds,
            max_retries: 3,
            retry_delay_ms: 100,
        })
    }

    pub fn create_client(&self) -> Result<RedisClient, AppError> {
        RedisClient::new(&self.redis_url)
            .map_err(|e| AppError::ConfigurationError(format!("Failed to create Redis client: {}", e)))
    }

    pub fn get_oauth_state_ttl(&self) -> Duration {
        Duration::from_secs(self.oauth_state_ttl_seconds as u64)
    }

    pub fn get_rate_limit_window(&self) -> Duration {
        Duration::from_secs(self.rate_limit_window_seconds)
    }

    pub fn validate(&self) -> Result<(), AppError> {
        if self.redis_url.is_empty() {
            return Err(AppError::ConfigurationError("Redis URL cannot be empty".into()));
        }

        if self.rate_limit_requests == 0 {
            return Err(AppError::ConfigurationError("Rate limit requests must be greater than 0".into()));
        }

        if self.rate_limit_window_seconds == 0 {
            return Err(AppError::ConfigurationError("Rate limit window must be greater than 0".into()));
        }

        if self.oauth_state_ttl_seconds <= 0 {
            return Err(AppError::ConfigurationError("OAuth state TTL must be greater than 0".into()));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_config_from_env() {
        // Set environment variables for testing
        unsafe {
            std::env::set_var("REDIS_URL", "redis://localhost:6379");
            std::env::set_var("RATE_LIMIT_REQUESTS", "100");
            std::env::set_var("RATE_LIMIT_WINDOW_SECONDS", "60");
            std::env::set_var("OAUTH_STATE_TTL", "300");
        }

        let config = RedisConfig::from_env();
        assert!(config.is_ok());

        let config = config.unwrap();
        assert_eq!(config.redis_url, "redis://localhost:6379");
        assert_eq!(config.rate_limit_requests, 100);
        assert_eq!(config.rate_limit_window_seconds, 60);
        assert_eq!(config.oauth_state_ttl_seconds, 300);
    }

    #[test]
    fn test_redis_config_validation() {
        let mut config = RedisConfig {
            redis_url: "redis://localhost:6379".to_string(),
            rate_limit_requests: 100,
            rate_limit_window_seconds: 60,
            oauth_state_ttl_seconds: 300,
            max_retries: 3,
            retry_delay_ms: 100,
        };

        assert!(config.validate().is_ok());

        // Test invalid rate limit requests
        config.rate_limit_requests = 0;
        assert!(config.validate().is_err());

        // Test invalid rate limit window
        config.rate_limit_requests = 100;
        config.rate_limit_window_seconds = 0;
        assert!(config.validate().is_err());

        // Test invalid OAuth state TTL
        config.rate_limit_window_seconds = 60;
        config.oauth_state_ttl_seconds = 0;
        assert!(config.validate().is_err());

        // Test empty Redis URL
        config.oauth_state_ttl_seconds = 300;
        config.redis_url = "".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_ttl_durations() {
        let config = RedisConfig {
            redis_url: "redis://localhost:6379".to_string(),
            rate_limit_requests: 100,
            rate_limit_window_seconds: 60,
            oauth_state_ttl_seconds: 300,
            max_retries: 3,
            retry_delay_ms: 100,
        };

        let oauth_ttl = config.get_oauth_state_ttl();
        assert_eq!(oauth_ttl.as_secs(), 300);

        let rate_limit_window = config.get_rate_limit_window();
        assert_eq!(rate_limit_window.as_secs(), 60);
    }
}