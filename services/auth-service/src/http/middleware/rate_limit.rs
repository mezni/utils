use actix_web::{dev::ServiceRequest, Error, HttpResponse};
use actix_web::dev::{Service, ServiceResponse, Transform};
use futures::future::{ok, Ready, LocalBoxFuture};
use std::sync::Arc;
use std::task::{Context, Poll};

use bornemap_db::{RedisClient, RedisKeys};

#[derive(Clone)]
pub struct RateLimitConfig {
    pub max_requests: u64,
    pub window_seconds: u64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_requests: 100,
            window_seconds: 60,
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
        let config = self.config.clone();
        let redis_client = self.redis_client.clone();

        Box::pin(async move {
            // Get client IP
            let client_ip = req
                .peer_addr()
                .map(|addr| addr.ip().to_string())
                .unwrap_or_else(|| "unknown".to_string());

            let key = RedisKeys::rate_limit(&client_ip);

            // Check rate limit
            let current_count = redis_client.increment(&key).await;
            let count = current_count.unwrap_or(0);

            // Set TTL on first request
            if count == 1 {
                let _ = redis_client.set_with_ttl(&key, &count.to_string(), config.window_seconds).await;
            }

            if count > config.max_requests {
                let response = HttpResponse::TooManyRequests()
                    .insert_header(("Retry-After", config.window_seconds.to_string()))
                    .json(serde_json::json!({
                        "error": "Rate limit exceeded",
                        "retry_after_seconds": config.window_seconds,
                    }));
                return Ok(req.into_response(response));
            }

            service.call(req).await
        })
    }
}