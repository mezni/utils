use actix_web::dev::ServiceRequest;
use actix_web::error::Error;
use actix_web::middleware::Next;
use actix_web::Response;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Rate limiting middleware for login endpoint.
///
/// Limits login attempts to 10 per minute per IP address.
pub struct RateLimitMiddleware {
    // In-memory storage for simplicity (use Redis in production)
    attempts: Arc<std::sync::RwLock<HashMap<String, Attempt>>>,
    limit: usize,
    window: Duration,
}

#[derive(Clone)]
struct Attempt {
    count: usize,
    first_attempt: Instant,
}

impl RateLimitMiddleware {
    /// Create a new rate limiting middleware.
    ///
    /// Args:
    /// - limit: Maximum number of attempts allowed
    /// - window: Time window for the limit
    pub fn new(limit: usize, window: Duration) -> Self {
        Self {
            attempts: Arc::new(std::sync::RwLock::new(HashMap::new())),
            limit,
            window,
        }
    }

    /// Create a default rate limiter: 10 attempts/minute.
    pub fn default() -> Self {
        Self::new(10, Duration::from_secs(60))
    }
}

impl<S, B> actix_web::dev::Service<S> for RateLimitMiddleware
where
    S: actix_web::dev::Service<ServiceRequest, Response = actix_web::HttpResponse, Error = Error>
        + 'static,
    S::Future: 'static,
{
    type Error = Error;
    type Service = S;

    fn actix_service(&mut self, service: S) -> Self::Service {
        service
    }

    fn call(&mut self, req: ServiceRequest, next: Next<'_, S>) -> Pin<Box<dyn Future<Output = Response> + 'static>> {
        let ip = req.peer_addr().map(|addr| addr.ip().to_string()).unwrap_or_else(|| "unknown".to_string());

        // Check if this is a login request
        if req.path().contains("/auth/login") {
            if let Err(e) = self.check_rate_limit(&ip) {
                let error_response = actix_web::HttpResponse::TooManyRequests().json(serde_json::json!({
                    "error": "rate_limit_exceeded",
                    "message": "Too many login attempts. Please try again later.",
                }));
                return Box::pin(async move { error_response.into() });
            }
        }

        Box::pin(async move {
            let response = next.call(req).await;
            response
        })
    }
}

impl RateLimitMiddleware {
    /// Check if the rate limit is exceeded for the given IP.
    fn check_rate_limit(&self, ip: &str) -> Result<(), AuthError> {
        let window = self.window;
        let limit = self.limit;

        // Use read lock for checking
        let attempts = self.attempts.read().unwrap();
        let attempt = attempts.get(ip);

        match attempt {
            Some(attempt) => {
                // Check if we need to reset
                if attempt.count >= limit {
                    if attempt.first_attempt.elapsed() >= window {
                        // Reset after window has passed
                        drop(attempts);
                        self.reset_attempts(ip);
                        return Ok(());
                    } else {
                        return Err(AuthError::ValidationError(
                            "Too many login attempts. Please try again later.".to_string(),
                        ));
                    }
                }
                Ok(())
            }
            None => Ok(()),
        }
    }

    /// Record a successful login attempt.
    fn record_success(&self, ip: &str) {
        let mut attempts = self.attempts.write().unwrap();
        attempts.insert(ip.to_string(), Attempt {
            count: 1,
            first_attempt: Instant::now(),
        });
    }

    /// Reset attempts for an IP (after window expires).
    fn reset_attempts(&self, ip: &str) {
        let mut attempts = self.attempts.write().unwrap();
        attempts.remove(ip);
    }
}

/// Helper function to get rate limiting middleware.
///
/// This function is typically called during middleware setup.
pub fn get_rate_limiter() -> RateLimitMiddleware {
    RateLimitMiddleware::default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limit_initial() {
        let limiter = RateLimitMiddleware::new(10, Duration::from_secs(60));
        let ip = "192.168.1.1";

        // First request should succeed
        let result = limiter.check_rate_limit(ip);
        assert!(result.is_ok());

        // Record a success
        limiter.record_success(ip);

        // Second request should still succeed
        let result = limiter.check_rate_limit(ip);
        assert!(result.is_ok());
    }

    #[test]
    fn test_rate_limit_exceeded() {
        let limiter = RateLimitMiddleware::new(2, Duration::from_secs(60));
        let ip = "192.168.1.1";

        // Make 3 requests
        let _ = limiter.check_rate_limit(ip); // First
        let _ = limiter.check_rate_limit(ip); // Second
        let result = limiter.check_rate_limit(ip); // Third should fail
        assert!(matches!(result, Err(AuthError::ValidationError(_))));
    }

    #[test]
    fn test_rate_limit_reset_after_window() {
        let limiter = RateLimitMiddleware::new(2, Duration::from_secs(1));
        let ip = "192.168.1.1";

        // Make 2 requests
        let _ = limiter.check_rate_limit(ip); // First
        let _ = limiter.check_rate_limit(ip); // Second
        let result = limiter.check_rate_limit(ip); // Third should fail
        assert!(matches!(result, Err(AuthError::ValidationError(_))));

        // Sleep for window duration + small buffer
        std::thread::sleep(Duration::from_millis(1100));

        // Reset should be called automatically, so request should succeed again
        let result = limiter.check_rate_limit(ip);
        assert!(result.is_ok());
    }
}
