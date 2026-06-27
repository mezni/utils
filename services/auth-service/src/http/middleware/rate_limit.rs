use actix_web::{dev::ServiceRequest, Error, HttpResponse};
use actix_web::dev::{Service, ServiceResponse, Transform};
use futures::future::{ok, Ready, LocalBoxFuture};
use std::sync::Arc;
use std::task::{Context, Poll};

use bornemap_db::{RedisClient, RedisKeys};
use serde_json;

#[derive(Clone)]
pub struct RateLimitConfig {
    pub ip_limit: u64,
    pub user_limit: u64,
    pub window_seconds: u64,
    pub sensitive_endpoint_multiplier: u64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            ip_limit: 100,
            user_limit: 25,
            window_seconds: 60,
            sensitive_endpoint_multiplier: 4,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RateLimits {
    pub ip_limit: u64,
    pub user_limit: u64,
    pub window_seconds: u64,
}

impl RateLimitConfig {
    fn get_rate_limits_for_path(&self, path: &str) -> RateLimits {
        let is_sensitive = path.contains("/login") || path.contains("/auth") || path.contains("/register");

        RateLimits {
            ip_limit: if is_sensitive {
                self.ip_limit / self.sensitive_endpoint_multiplier
            } else {
                self.ip_limit
            },
            user_limit: if is_sensitive {
                self.user_limit / self.sensitive_endpoint_multiplier
            } else {
                self.user_limit
            },
            window_seconds: self.window_seconds,
        }
    }
}

#[derive(Clone)]
pub struct RateLimitMiddlewareFactory {
    config: RateLimitConfig,
    redis_client: Arc<RedisClient>,
}

impl RateLimitMiddlewareFactory {
    pub fn new(config: RateLimitConfig, redis_client: Arc<RedisClient>) -> Self {
        Self { config, redis_client }
    }
}

impl<S> Transform<S, ServiceRequest> for RateLimitMiddlewareFactory
where
    S: Service<ServiceRequest, Response = ServiceResponse, Error = Error> + 'static,
    S::Future: 'static,
{
    type Response = ServiceResponse;
    type Error = Error;
    type Transform = RateLimitMiddleware<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ok(RateLimitMiddleware {
            service: Arc::new(service),
            config: self.config.clone(),
            redis_client: self.redis_client.clone(),
        })
    }
}

pub struct RateLimitMiddleware<S> {
    service: Arc<S>,
    config: RateLimitConfig,
    redis_client: Arc<RedisClient>,
}

impl<S> Service<ServiceRequest> for RateLimitMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse, Error = Error> + 'static,
    S::Future: 'static,
{
    type Response = ServiceResponse;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let service = self.service.clone();
        
        Box::pin(async move {
            // TODO: Implement rate limiting
            // For now, just pass the request through
            service.call(req).await
        })
    }
}

impl<S> RateLimitMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse, Error = Error> + 'static,
{
    async fn record_rate_limit_success(&self, ip_key: &str, ip_count: &u64, window_seconds: &u64) {
        // Optional: Implement telemetry for rate limit hits
        // This could be used for monitoring and analyzing attack patterns
        let _ = ip_key;
        let _ = ip_count;
        let _ = window_seconds;
    }

    async fn record_rate_limit_exceeded(&self, key: &str, count: &u64, window_seconds: &u64) {
        // Increment failure counter for this key
        let failure_key = format!("rate_limit_failures:{}", key);
        let failure_count = self.redis_client.increment(&failure_key).await.unwrap_or(0);

        if failure_count == 1 {
            // Set TTL for failure tracking
            let _ = self.redis_client.set_with_ttl(&failure_key, &failure_count.to_string(), window_seconds * 2).await;
        }

        // If too many failures, implement additional blocking
        if failure_count >= 5 {
            let blocking_key = format!("rate_limit_blocked:{}", key);
            let _ = self.redis_client.set_with_ttl(&blocking_key, "1", window_seconds * 3).await;

            // Optional: Log suspicious activity
            tracing::warn!("Rate limit exceeded - IP: {}, failures: {}, key: {}", key, failure_count, key);
        }
    }
}
