use actix_web::{HttpResponse, ResponseError};
use std::fmt;

#[derive(Debug)]
pub enum AppError {
    NotFound(String),
    BadRequest(String),
    #[allow(dead_code)]
    InternalError(String),
    DbError(String),
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::NotFound(msg) => write!(f, "Not found: {}", msg),
            AppError::BadRequest(msg) => write!(f, "Bad request: {}", msg),
            AppError::InternalError(msg) => write!(f, "Internal error: {}", msg),
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
            AppError::InternalError(msg) => {
                (actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, "internal_error", msg.clone())
            }
            AppError::DbError(msg) => {
                (actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, "db_error", msg.clone())
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
