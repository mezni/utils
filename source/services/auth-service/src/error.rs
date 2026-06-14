use actix_web::{error::ResponseError, http::StatusCode, HttpResponse};
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AuthServiceError {
    #[error("Invalid credentials")]
    InvalidCredentials,

    #[error("User already exists")]
    UserAlreadyExists,

    #[error("User not found")]
    UserNotFound,

    #[error("Invalid or expired token")]
    InvalidToken,

    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Password hashing failed")]
    PasswordError,

    #[error("JWT generation failed")]
    JwtError,

    #[error("Internal server error")]
    InternalError,
}

impl ResponseError for AuthServiceError {
    fn error_response(&self) -> HttpResponse {
        match self {
            AuthServiceError::InvalidCredentials => {
                HttpResponse::Unauthorized().json(json!({
                    "error": "INVALID_CREDENTIALS",
                    "message": "Email or password is incorrect"
                }))
            }
            AuthServiceError::UserAlreadyExists => {
                HttpResponse::Conflict().json(json!({
                    "error": "USER_EXISTS",
                    "message": "An account with this email already exists"
                }))
            }
            AuthServiceError::UserNotFound => {
                HttpResponse::NotFound().json(json!({
                    "error": "NOT_FOUND",
                    "message": "User not found"
                }))
            }
            AuthServiceError::InvalidToken => {
                HttpResponse::Unauthorized().json(json!({
                    "error": "INVALID_TOKEN",
                    "message": "Token is invalid or has expired"
                }))
            }
            AuthServiceError::DatabaseError(_) => {
                HttpResponse::InternalServerError().json(json!({
                    "error": "DATABASE_ERROR",
                    "message": "Database operation failed"
                }))
            }
            AuthServiceError::PasswordError => {
                HttpResponse::InternalServerError().json(json!({
                    "error": "PASSWORD_ERROR",
                    "message": "Password hashing failed"
                }))
            }
            AuthServiceError::JwtError => {
                HttpResponse::InternalServerError().json(json!({
                    "error": "JWT_ERROR",
                    "message": "Token generation failed"
                }))
            }
            AuthServiceError::InternalError => {
                HttpResponse::InternalServerError().json(json!({
                    "error": "INTERNAL_ERROR",
                    "message": "An unexpected error occurred"
                }))
            }
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            AuthServiceError::InvalidCredentials => StatusCode::UNAUTHORIZED,
            AuthServiceError::UserAlreadyExists => StatusCode::CONFLICT,
            AuthServiceError::UserNotFound => StatusCode::NOT_FOUND,
            AuthServiceError::InvalidToken => StatusCode::UNAUTHORIZED,
            AuthServiceError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AuthServiceError::PasswordError => StatusCode::INTERNAL_SERVER_ERROR,
            AuthServiceError::JwtError => StatusCode::INTERNAL_SERVER_ERROR,
            AuthServiceError::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
