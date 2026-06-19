use actix_web::{error::ResponseError, http::StatusCode, HttpResponse, Json};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

#[derive(Debug)]
pub enum AuthError {
    ValidationError(String),
    Unauthorized,
    Forbidden(String),
    Conflict(String),
    NotFound(String),
    InternalError,
}

impl AuthError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            AuthError::ValidationError(_) => StatusCode::BAD_REQUEST,
            AuthError::Unauthorized => StatusCode::UNAUTHORIZED,
            AuthError::Forbidden(_) => StatusCode::FORBIDDEN,
            AuthError::Conflict(_) => StatusCode::CONFLICT,
            AuthError::NotFound(_) => StatusCode::NOT_FOUND,
            AuthError::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn error_code(&self) -> String {
        match self {
            AuthError::ValidationError(_) => "validation_error".to_string(),
            AuthError::Unauthorized => "unauthorized".to_string(),
            AuthError::Forbidden(_) => "forbidden".to_string(),
            AuthError::Conflict(_) => "constraint_violation".to_string(),
            AuthError::NotFound(_) => "not_found".to_string(),
            AuthError::InternalError => "internal_error".to_string(),
        }
    }
}

impl ResponseError for AuthError {
    fn error_response(&self) -> HttpResponse {
        let status_code = self.status_code();
        let error_code = self.error_code();

        let response = ErrorResponse {
            error: error_code.clone(),
            details: match self {
                AuthError::ValidationError(msg) => Some(serde_json::json!({
                    "message": msg
                })),
                AuthError::Forbidden(msg) => Some(serde_json::json!({
                    "message": msg.clone(),
                    "required_role": "role:admin"
                })),
                AuthError::NotFound(msg) => Some(serde_json::json!({
                    "message": msg.clone(),
                    "entity_type": "unknown"
                })),
                _ => None,
            },
        };

        HttpResponse::build(status_code).json(response)
    }
}
