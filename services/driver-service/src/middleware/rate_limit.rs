use std::collections::HashMap;
use std::time::{Duration, Instant};

pub struct RateLimiter {
    requests: HashMap<String, Vec<Instant>>,
    max_requests: usize,
    window_seconds: u64,
}

impl RateLimiter {
    pub fn new(max_requests: usize, window_seconds: u64) -> Self {
        RateLimiter {
            requests: HashMap::new(),
            max_requests,
            window_seconds,
        }
    }

    pub fn check(&mut self, user_id: &str) -> bool {
        let now = Instant::now();
        let window = Duration::from_secs(self.window_seconds);

        let user_requests = self.requests.entry(user_id.to_string()).or_insert_with(Vec::new);

        // Remove old requests outside the window
        user_requests.retain(|&timestamp| now.duration_since(timestamp) < window);

        // Check if limit is exceeded
        if user_requests.len() >= self.max_requests {
            return false;
        }

        user_requests.push(now);
        true
    }
}

// Rate limit middleware
pub async fn rate_limit_middleware(
    rate_limiter: &mut RateLimiter,
    user_id: &str,
) -> Result<(), ErrorResponse> {
    if !rate_limiter.check(user_id) {
        Err(ErrorResponse {
            error: ErrorDetail {
                code: "RATE_LIMIT_EXCEEDED".to_string(),
                message: "Too many requests. Please try again later.".to_string(),
                field: None,
            },
            meta: ResponseMeta {
                request_id: uuid::Uuid::new_v4().to_string(),
                timestamp: chrono::Utc::now().to_rfc3339(),
            },
        })
    } else {
        Ok(())
    }
}
