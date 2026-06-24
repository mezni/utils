use serde::Serialize;

pub use crate::error::{AppError, AppResult, internal_error, not_found_error, validation_error};

#[derive(Debug, Clone, Serialize)]
pub struct ApiResponse<T: Serialize> {
    pub success: bool,
    pub data: T,
    pub error: Option<ErrorDetail>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ErrorResponse {
    pub success: bool,
    pub data: Option<()>,
    pub error: Option<ErrorDetail>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ErrorDetail {
    pub code: String,
    pub message: String,
}

impl<T: Serialize> ApiResponse<T> {
    pub fn ok(data: T) -> Self {
        Self {
            success: true,
            data,
            error: None,
        }
    }
}

impl ErrorResponse {
    pub fn error(code: &str, message: &str) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(ErrorDetail {
                code: code.to_string(),
                message: message.to_string(),
            }),
        }
    }
}

pub fn to_error_response(err: &AppError) -> ErrorResponse {
    match err {
        AppError::Validation(msg) => ErrorResponse::error("VALIDATION_ERROR", msg),
        AppError::NotFound(msg) => ErrorResponse::error("NOT_FOUND", msg),
        AppError::Database(_) => ErrorResponse::error("DB_ERROR", "A database error occurred"),
        AppError::Internal(msg) => ErrorResponse::error("INTERNAL_ERROR", msg),
        AppError::Serialization(e) => ErrorResponse::error("INTERNAL_ERROR", &e.to_string()),
        AppError::Configuration(msg) => ErrorResponse::error("INTERNAL_ERROR", msg),
        AppError::Io(e) => ErrorResponse::error("INTERNAL_ERROR", &e.to_string()),
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct KpiData {
    pub partners_count: i64,
    pub stations_count: i64,
    pub chargers_count: i64,
}
