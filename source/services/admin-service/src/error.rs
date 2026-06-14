use actix_web::{error::ResponseError, http::StatusCode, HttpResponse};
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AdminServiceError {
    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Internal server error")]
    InternalError,
}

impl ResponseError for AdminServiceError {
    fn error_response(&self) -> HttpResponse {
        match self {
            AdminServiceError::InvalidRequest(msg) => {
                HttpResponse::BadRequest().json(json!({
                    "error": "INVALID_REQUEST",
                    "message": msg
                }))
            }
            AdminServiceError::NotFound(msg) => {
                HttpResponse::NotFound().json(json!({
                    "error": "NOT_FOUND",
                    "message": msg
                }))
            }
            AdminServiceError::Conflict(msg) => {
                HttpResponse::Conflict().json(json!({
                    "error": "CONFLICT",
                    "message": msg
                }))
            }
            AdminServiceError::DatabaseError(_) => {
                HttpResponse::InternalServerError().json(json!({
                    "error": "DATABASE_ERROR",
                    "message": "Failed to perform database operation"
                }))
            }
            AdminServiceError::InternalError => {
                HttpResponse::InternalServerError().json(json!({
                    "error": "INTERNAL_ERROR",
                    "message": "An unexpected error occurred"
                }))
            }
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            AdminServiceError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            AdminServiceError::NotFound(_) => StatusCode::NOT_FOUND,
            AdminServiceError::Conflict(_) => StatusCode::CONFLICT,
            AdminServiceError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AdminServiceError::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
