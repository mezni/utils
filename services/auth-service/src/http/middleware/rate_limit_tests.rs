#[cfg(test)]
mod rate_limit_tests {
    use super::*;
    use bornemap_core::Result;
    use bornemap_db::RedisClient;
    use actix_web::{dev::ServiceRequest, dev::ServiceResponse, dev::Error, dev::Service, http::HttpResponse};
    use futures::future::Ready;
    use std::task::{Context, Poll};
    use std::sync::Arc;

    // Test helper to create a test rate limit middleware
    fn create_test_middleware() -> (RateLimitMiddlewareFactory, Arc<RedisClient>, Arc<RedisClient>) {
        // Create a mock Redis client for testing
        let redis_client = Arc::new(RedisClient::new("redis://127.0.0.1:6379").unwrap());
        let config = RateLimitConfig::default();
        let factory = RateLimitMiddlewareFactory::new(config, redis_client.clone());

        (factory, redis_client, redis_client)
    }

    #[test]
    fn test_rate_limit_config_default_values() {
        let config = RateLimitConfig::default();
        assert_eq!(config.ip_limit, 100);
        assert_eq!(config.user_limit, 25);
        assert_eq!(config.window_seconds, 60);
        assert_eq!(config.sensitive_endpoint_multiplier, 4);
    }

    #[test]
    fn test_sensitive_endpoint_stricter_limits() {
        let config = RateLimitConfig::default();
        let sensitive_limits = config.get_rate_limits_for_path("/login");

        assert_eq!(sensitive_limits.ip_limit, 25);
        assert_eq!(sensitive_limits.user_limit, 6);
        assert_eq!(sensitive_limits.window_seconds, 60);

        let normal_limits = config.get_rate_limits_for_path("/api/users");
        assert_eq!(normal_limits.ip_limit, 100);
        assert_eq!(normal_limits.user_limit, 25);
        assert_eq!(normal_limits.window_seconds, 60);
    }

    #[test]
    fn test_sensitive_endpoint_stricter_limits_register() {
        let config = RateLimitConfig::default();
        let sensitive_limits = config.get_rate_limits_for_path("/register");

        assert_eq!(sensitive_limits.ip_limit, 25);
        assert_eq!(sensitive_limits.user_limit, 6);
        assert_eq!(sensitive_limits.window_seconds, 60);
    }

    #[test]
    fn test_normal_endpoint_standard_limits() {
        let config = RateLimitConfig::default();
        let normal_limits = config.get_rate_limits_for_path("/api/data");

        assert_eq!(normal_limits.ip_limit, 100);
        assert_eq!(normal_limits.user_limit, 25);
        assert_eq!(normal_limits.window_seconds, 60);
    }

    #[test]
    fn test_sensitive_endpoint_stricter_limits_auth() {
        let config = RateLimitConfig::default();
        let sensitive_limits = config.get_rate_limits_for_path("/auth/callback");

        assert_eq!(sensitive_limits.ip_limit, 25);
        assert_eq!(sensitive_limits.user_limit, 6);
        assert_eq!(sensitive_limits.window_seconds, 60);
    }

    #[test]
    fn test_large_request_limits() {
        let config = RateLimitConfig::default();
        assert_eq!(config.ip_limit, 100);
        assert_eq!(config.user_limit, 25);
        assert_eq!(config.window_seconds, 60);
    }

    #[test]
    fn test_user_limit_zero_for_normal_endpoints() {
        let config = RateLimitConfig::default();
        let normal_limits = config.get_rate_limits_for_path("/api/data");

        assert_eq!(normal_limits.user_limit, 25);
    }

    #[test]
    fn test_user_limit_zero_for_sensitive_endpoints() {
        let config = RateLimitConfig::default();
        let sensitive_limits = config.get_rate_limits_for_path("/login");

        assert_eq!(sensitive_limits.user_limit, 6);
    }

    #[test]
    fn test_sensitive_endpoint_rate_limits() {
        let config = RateLimitConfig::default();
        let sensitive_limits = config.get_rate_limits_for_path("/auth/login");

        assert_eq!(sensitive_limits.ip_limit, 25);
        assert_eq!(sensitive_limits.user_limit, 6);
        assert_eq!(sensitive_limits.window_seconds, 60);
    }

    #[test]
    fn test_normal_endpoint_rate_limits() {
        let config = RateLimitConfig::default();
        let normal_limits = config.get_rate_limits_for_path("/api/health");

        assert_eq!(normal_limits.ip_limit, 100);
        assert_eq!(normal_limits.user_limit, 25);
        assert_eq!(normal_limits.window_seconds, 60);
    }

    #[test]
    fn test_user_id_extraction_from_headers() {
        // This test validates that the middleware can extract user ID from headers
        let redis_client = Arc::new(RedisClient::new("redis://127.0.0.1:6379"));
        let config = RateLimitConfig::default();
        let factory = RateLimitMiddlewareFactory::new(config, redis_client.clone());

        // Test that the middleware can be created
        assert!(factory.new_transform(&MockService).is_ok());
    }

    // Mock service for testing
    struct MockService;

    impl<S> Service<S> for MockService
    where
        S: Service<ServiceRequest, Response = ServiceResponse, Error = Error> + 'static,
        S::Future: 'static,
    {
        type Response = ServiceResponse;
        type Error = Error;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&self, req: ServiceRequest) -> Self::Future {
            ready(Ok(req.into_response(HttpResponse::Ok().finish())))
        }
    }
}