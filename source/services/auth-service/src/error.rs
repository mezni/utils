use actix_web::{error::ResponseError, http::StatusCode, HttpResponse};
use serde::Serialize;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AuthError {
    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Invalid credentials")]
    InvalidCredentials,

    #[error("Token expired")]
    TokenExpired,

    #[error("Authentication unavailable")]
    AuthUnavailable,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Vec<ErrorDetail>>,
}

#[derive(Debug, Serialize)]
pub struct ErrorDetail {
    pub field: String,
    pub message: String,
}

impl AuthError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            AuthError::ValidationError(_) => StatusCode::BAD_REQUEST,
            AuthError::InvalidCredentials => StatusCode::UNAUTHORIZED,
            AuthError::TokenExpired => StatusCode::UNAUTHORIZED,
            AuthError::AuthUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    pub fn error_code(&self) -> String {
        match self {
            AuthError::ValidationError(_) => "validation_error".to_string(),
            AuthError::InvalidCredentials => "invalid_credentials".to_string(),
            AuthError::TokenExpired => "token_expired".to_string(),
            AuthError::AuthUnavailable => "auth_unavailable".to_string(),
        }
    }
}

impl ResponseError for AuthError {
    fn error_response(&self) -> HttpResponse {
        let error_code = self.error_code();
        let details = match self {
            AuthError::ValidationError(msg) => Some(vec![ErrorDetail {
                field: "error".to_string(),
                message: msg.clone(),
            }]),
            _ => None,
        };

        let response = ErrorResponse {
            error: error_code,
            details,
        };

        HttpResponse::build(self.status_code()).json(response)
    }
}
