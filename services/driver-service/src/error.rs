use actix_web::{HttpResponse, ResponseError};
use serde::Serialize;
use std::fmt;

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
                HttpResponse::BadRequest().json(serde_json::json!({ "error": msg }))
            }
            AppError::DatabaseError(msg) => {
                HttpResponse::InternalServerError().json(serde_json::json!({ "error": msg }))
            }
            AppError::HealthCheckError(msg) => {
                HttpResponse::InternalServerError().json(serde_json::json!({ "error": msg }))
            }
        }
    }
}

#[derive(Debug)]
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

impl ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        let status = actix_web::http::StatusCode::from_u16(self.status_code)
            .unwrap_or(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR);
        HttpResponse::build(status).json(serde_json::json!({ "error": self.error }))
    }
}
