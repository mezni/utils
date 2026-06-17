use actix_web::{web, HttpResponse, ResponseError};
use sqlx::PgPool;
use std::env;
use crate::handler::import::import_handler;
use crate::handler::nearby::nearby_handler;
use crate::middleware::auth::verify_jwt;
use crate::middleware::rate_limit::RateLimiter;
use crate::models::error::{ErrorResponse, ErrorDetail, Result, ResponseMeta};

// Global rate limiter instance
static mut RATE_LIMITER: Option<RateLimiter> = None;

pub async fn setup_routes(
    app: &mut web::ServiceConfig,
    pool: web::Data<PgPool>,
) {
    // Get rate limit from environment
    let max_requests = env::var("RATE_LIMIT_MAX").ok().and_then(|v| v.parse().ok()).unwrap_or(100);
    let window_seconds = env::var("RATE_LIMIT_WINDOW").ok().and_then(|v| v.parse().ok()).unwrap_or(60);

    // Initialize rate limiter
    unsafe {
        RATE_LIMITER = Some(RateLimiter::new(max_requests, window_seconds));
    }

    // Nearby stations endpoint
    app.service(
        web::scope("/api/v1/nearby")
            .route("", web::get().to(nearby_handler))
    );

    // Import endpoint
    app.service(
        web::scope("/api/v1")
            .route("/import", web::post().to(import_handler))
    );
}

impl ResponseError for ErrorResponse {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::BadRequest().json(self)
    }
}
