use actix_web::{HttpResponse, ResponseError};
use log::error;
use std::fmt;

#[derive(Debug)]
pub enum AppError {
    NotFound(String),
    BadRequest(String),
    DbError(String),
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::NotFound(msg) => write!(f, "Not found: {}", msg),
            AppError::BadRequest(msg) => write!(f, "Bad request: {}", msg),
            AppError::DbError(msg) => write!(f, "Database error: {}", msg),
        }
    }
}

impl ResponseError for AppError {
    fn error_response(&self) -> HttpResponse {
        let (status, code, message) = match self {
            AppError::NotFound(msg) => {
                (actix_web::http::StatusCode::NOT_FOUND, "not_found", msg.clone())
            }
            AppError::BadRequest(msg) => {
                (actix_web::http::StatusCode::BAD_REQUEST, "bad_request", msg.clone())
            }
            AppError::DbError(msg) => {
                error!("Database error (logged, not returned to client): {msg}");
                (
                    actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "An internal error occurred".to_string(),
                )
            }
        };

        HttpResponse::build(status).json(serde_json::json!({
            "error": {
                "code": code,
                "message": message,
            }
        }))
    }
}

impl From<sqlx::Error> for AppError {
    fn from(err: sqlx::Error) -> Self {
        AppError::DbError(err.to_string())
    }
}
