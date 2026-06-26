use actix_web::{dev::ServiceRequest, dev::ServiceResponse, Error, HttpResponse, body::EitherBody};
use actix_web::body::BoxBody;
use actix_web::http::StatusCode;
use actix_web::middleware::{Next, Transform};
use actix_web::web::Data;
use futures::future::{ok, Ready, LocalBoxFuture};
use std::sync::Arc;
use std::time::Duration;
use bornemap_db::{RedisClient, RedisKeys};
use bornemap_core::AppError;
use tracing::{error, warn};

#[derive(Clone)]
pub struct RateLimitConfig {
    pub requests_per_window: u32,
    pub window_seconds: u64,
}

impl RateLimitConfig {
    pub fn new(requests_per_window: u32, window_seconds: u64) -> Self {
        Self {
            requests_per_window,
            window_seconds,
        }
    }

    pub fn from_env() -> Result<Self, AppError> {
        let requests_per_window = std::env::var("RATE_LIMIT_REQUESTS")
            .unwrap_or_else(|_| "100".into())
            .parse()
            .map_err(|_| AppError::ConfigurationError("Invalid RATE_LIMIT_REQUESTS".into()))?;

        let window_seconds = std::env::var("RATE_LIMIT_WINDOW_SECONDS")
            .unwrap_or_else(|_| "60".into())
            .parse()
            .map_err(|_| AppError::ConfigurationError("Invalid RATE_LIMIT_WINDOW_SECONDS".into()))?;

        Ok(Self::new(requests_per_window, window_seconds))
    }
}

pub struct RateLimitMiddleware {
    config: RateLimitConfig,
    redis_client: Arc<RedisClient>,
}

impl RateLimitMiddleware {
    pub fn new(config: RateLimitConfig, redis_client: RedisClient) -> Self {
        Self {
            config,
            redis_client: Arc::new(redis_client),
        }
    }

    pub fn with_default(redis_client: RedisClient) -> Result<Self, AppError> {
        let config = RateLimitConfig::from_env()?;
        Ok(Self::new(config, redis_client))
    }

    /// Extract client identifier from request (IP address or trusted proxy)
    fn extract_client_identifier(&self, req: &ServiceRequest) -> String {
        // Try to get the real IP from X-Forwarded-For header if present
        if let Some(forwarded_for) = req.headers().get("X-Forwarded-For") {
            if let Ok(forwarded_str) = forwarded_for.to_str() {
                // X-Forwarded-For can contain multiple IPs, take the first one
                let first_ip = forwarded_str.split(',').next().unwrap_or("").trim();
                if !first_ip.is_empty() {
                    return format!("ip:{}", first_ip);
                }
            }
        }

        // Fallback to remote address
        if let Some(remote_addr) = req.connection_info().peer_addr() {
            format!("ip:{}", remote_addr)
        } else {
            // Fallback to a default if we can't determine the IP
            format!("ip:unknown")
        }
    }

    /// Check if the client is rate limited
    async fn check_rate_limit(&self, client_id: &str) -> Result<bool, AppError> {
        let key = RedisKeys::rate_limit(client_id);
        let window = Duration::from_secs(self.config.window_seconds);

        // Try to set the key if it doesn't exist (atomic operation)
        let set_result = self.redis_client
            .set_if_not_exists(&key, "1", window.as_secs())
            .await?;

        if set_result {
            // Key didn't exist, this is the first request in the window
            return Ok(false); // Not rate limited
        }

        // Key exists, increment the counter
        let count = self.redis_client
            .increment(&key)
            .await?;

        // Check if we've exceeded the limit
        if count > self.config.requests_per_window as i64 {
            return Ok(true); // Rate limited
        }

        Ok(false) // Not rate limited
    }
}

impl<S, B> Transform<S, ServiceRequest> for RateLimitMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<BoxBody>>;
    type Error = Error;
    type Transform = RateLimitService<S>;
    type InitError = ();
    type Future = Ready<Result<(Self::Transform, ()), Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ok(RateLimitService {
            service,
            config: self.config.clone(),
            redis_client: self.redis_client.clone(),
        })
    }
}

pub struct RateLimitService<S> {
    service: S,
    config: RateLimitConfig,
    redis_client: Arc<RedisClient>,
}

impl<S, B> Service<ServiceRequest> for RateLimitService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<BoxBody>>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let service = self.service.clone();
        let config = self.config.clone();
        let redis_client = self.redis_client.clone();

        Box::pin(async move {
            // Extract client identifier
            let client_id = RateLimitMiddleware::new(config, (*redis_client).clone())
                .extract_client_identifier(&req);

            // Check rate limit
            match RateLimitMiddleware::new(config.clone(), (*redis_client).clone())
                .check_rate_limit(&client_id).await
            {
                Ok(true) => {
                    // Rate limit exceeded
                    warn!("Rate limit exceeded for client: {}", client_id);
                    let response = HttpResponse::TooManyRequests()
                        .json(serde_json::json!({
                            "error": {
                                "code": "RATE_LIMIT_EXCEEDED",
                                "message": "Too many requests. Please try again later."
                            },
                            "meta": {
                                "request_id": req.headers()
                                    .get("X-Request-ID")
                                    .and_then(|h| h.to_str().ok())
                                    .unwrap_or("unknown"),
                                "timestamp": chrono::Utc::now().to_rfc3339(),
                                "retry_after": config.window_seconds
                            }
                        }))
                        .map_into_left_body::<BoxBody>();
                    
                    Ok(req.into_response(response))
                }
                Ok(false) => {
                    // Not rate limited, continue to next service
                    service.call(req).await
                }
                Err(e) => {
                    // Redis error, log and allow request to continue (fail open)
                    error!("Redis error in rate limiting: {}. Allowing request to continue.", e);
                    service.call(req).await
                }
            }
        })
    }
}

/// Rate limiting configuration for different endpoints
pub struct EndpointRateLimits {
    pub register: RateLimitConfig,
    pub login: RateLimitConfig,
    pub oauth_start: RateLimitConfig,
    pub oauth_callback: RateLimitConfig,
}

impl EndpointRateLimits {
    pub fn default() -> Self {
        Self {
            register: RateLimitConfig::new(5, 300),  // 5 requests per 5 minutes
            login: RateLimitConfig::new(10, 300),   // 10 requests per 5 minutes
            oauth_start: RateLimitConfig::new(20, 60), // 20 requests per minute
            oauth_callback: RateLimitConfig::new(30, 60), // 30 requests per minute
        }
    }

    pub fn from_env() -> Result<Self, AppError> {
        Ok(Self::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test;
    use actix_web::App;

    #[test]
    fn test_rate_limit_config_from_env() {
        // Set environment variables
        std::env::set_var("RATE_LIMIT_REQUESTS", "100");
        std::env::set_var("RATE_LIMIT_WINDOW_SECONDS", "60");

        let config = RateLimitConfig::from_env();
        assert!(config.is_ok());

        let config = config.unwrap();
        assert_eq!(config.requests_per_window, 100);
        assert_eq!(config.window_seconds, 60);
    }

    #[test]
    fn test_endpoint_rate_limits_default() {
        let limits = EndpointRateLimits::default();
        
        assert_eq!(limits.register.requests_per_window, 5);
        assert_eq!(limits.register.window_seconds, 300);
        assert_eq!(limits.login.requests_per_window, 10);
        assert_eq!(limits.login.window_seconds, 300);
        assert_eq!(limits.oauth_start.requests_per_window, 20);
        assert_eq!(limits.oauth_start.window_seconds, 60);
        assert_eq!(limits.oauth_callback.requests_per_window, 30);
        assert_eq!(limits.oauth_callback.window_seconds, 60);
    }

    #[test]
    fn test_extract_client_identifier() {
        let config = RateLimitConfig::new(100, 60);
        let redis_client = MockRedisClient::new();
        let middleware = RateLimitMiddleware::new(config, redis_client);

        // Test with X-Forwarded-For header
        let req = MockServiceRequest::new().with_header("X-Forwarded-For", "192.168.1.1,10.0.0.1");
        let client_id = middleware.extract_client_identifier(&req);
        assert_eq!(client_id, "ip:192.168.1.1");

        // Test with remote address
        let req = MockServiceRequest::new().with_remote_addr("192.168.1.2:8080");
        let client_id = middleware.extract_client_identifier(&req);
        assert_eq!(client_id, "ip:192.168.1.2:8080");
    }

    // Mock structures for testing
    struct MockRedisClient;
    impl MockRedisClient {
        fn new() -> Self { Self }
    }

    impl bornemap_db::RedisClient for MockRedisClient {
        async fn set_with_ttl(&self, _key: &str, _value: &str, _ttl_seconds: u64) -> Result<(), bornemap_core::AppError> {
            Ok(())
        }

        async fn get(&self, _key: &str) -> Result<Option<String>, bornemap_core::AppError> {
            Ok(None)
        }

        async fn delete(&self, _key: &str) -> Result<(), bornemap_core::AppError> {
            Ok(())
        }

        async fn exists(&self, _key: &str) -> Result<bool, bornemap_core::AppError> {
            Ok(false)
        }

        async fn increment(&self, _key: &str) -> Result<i64, bornemap_core::AppError> {
            Ok(1)
        }

        async fn set_if_not_exists(&self, _key: &str, _value: &str, _ttl_seconds: u64) -> Result<bool, bornemap_core::AppError> {
            Ok(false)
        }

        async fn ttl(&self, _key: &str) -> Result<i64, bornemap_core::AppError> {
            Ok(-1)
        }

        async fn exists_and_valid(&self, _key: &str) -> Result<bool, bornemap_core::AppError> {
            Ok(false)
        }

        async fn close(&self) -> Result<(), bornemap_core::AppError> {
            Ok(())
        }

        async fn health_check(&self) -> Result<(), bornemap_core::AppError> {
            Ok(())
        }
    }

    struct MockServiceRequest {
        headers: std::collections::HashMap<String, String>,
        remote_addr: Option<String>,
    }

    impl MockServiceRequest {
        fn new() -> Self {
            Self {
                headers: std::collections::HashMap::new(),
                remote_addr: None,
            }
        }

        fn with_header(mut self, name: &str, value: &str) -> Self {
            self.headers.insert(name.to_string(), value.to_string());
            self
        }

        fn with_remote_addr(mut self, addr: &str) -> Self {
            self.remote_addr = Some(addr.to_string());
            self
        }
    }

    impl MockServiceRequest {
        fn headers(&self) -> &std::collections::HashMap<String, String> {
            &self.headers
        }

        fn connection_info(&self) -> MockConnectionInfo {
            MockConnectionInfo {
                peer_addr: self.remote_addr.as_deref(),
            }
        }
    }

    struct MockConnectionInfo {
        peer_addr: Option<&str>,
    }

    impl MockConnectionInfo {
        fn peer_addr(&self) -> Option<&str> {
            self.peer_addr
        }
    }
}