// Error types module
use actix_web::{error, ResponseError, HttpResponse};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Custom error types for the driver service
#[derive(Debug, Serialize)]
pub enum AppError {
    ValidationError(String),
    DatabaseError(String),
    HealthCheckError(String),
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::ValidationError(msg) => write!(f, "Invalid parameters: {}", msg),
            AppError::DatabaseError(msg) => write!(f, "Database query failed: {}", msg),
            AppError::HealthCheckError(msg) => write!(f, "Service error: {}", msg),
        }
    }
}

impl std::error::Error for AppError {}

impl ResponseError for AppError {
    fn error_response(&self) -> HttpResponse {
        match self {
            AppError::ValidationError(msg) => {
                HttpResponse::BadRequest().json(serde_json::json!({
                    "error": msg
                }))
            }
            AppError::DatabaseError(msg) => {
                HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": msg
                }))
            }
            AppError::HealthCheckError(msg) => {
                HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": msg
                }))
            }
        }
    }
}

/// API errors for integration tests
pub struct ApiError {
    pub status_code: u16,
    pub error: String,
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.status_code, self.error)
    }
}

impl std::error::Error for ApiError {}

impl error::ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::new(self.status_code).json(serde_json::json!({
            "error": self.error
        }))
    }
}
