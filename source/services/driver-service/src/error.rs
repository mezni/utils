use actix_web::{error::ResponseError, http::StatusCode, HttpResponse};
use serde_json::json;
use std::fmt;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DriverServiceError {
    #[error("Invalid coordinates: {0}")]
    InvalidCoordinates(String),

    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Location outside service bounds")]
    OutOfBounds,

    #[error("Internal server error")]
    InternalError,
}

impl ResponseError for DriverServiceError {
    fn error_response(&self) -> HttpResponse {
        match self {
            DriverServiceError::InvalidCoordinates(msg) => {
                HttpResponse::BadRequest().json(json!({
                    "error": "INVALID_COORDINATES",
                    "message": msg
                }))
            }
            DriverServiceError::OutOfBounds => {
                HttpResponse::BadRequest().json(json!({
                    "error": "OUT_OF_BOUNDS",
                    "message": "Location is outside the Tunisia service perimeter"
                }))
            }
            DriverServiceError::DatabaseError(_) => {
                HttpResponse::InternalServerError().json(json!({
                    "error": "DATABASE_ERROR",
                    "message": "Failed to query station data"
                }))
            }
            DriverServiceError::InternalError => {
                HttpResponse::InternalServerError().json(json!({
                    "error": "INTERNAL_ERROR",
                    "message": "An unexpected error occurred"
                }))
            }
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            DriverServiceError::InvalidCoordinates(_) => StatusCode::BAD_REQUEST,
            DriverServiceError::OutOfBounds => StatusCode::BAD_REQUEST,
            DriverServiceError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            DriverServiceError::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
