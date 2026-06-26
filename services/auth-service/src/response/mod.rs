use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub data: Option<T>,
    pub meta: ApiMeta,
    pub error: Option<ApiError>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiMeta {
    pub request_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiError {
    pub code: String,
    pub message: String,
    pub field: Option<String>,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T, request_id: String) -> Self {
        Self {
            data: Some(data),
            meta: ApiMeta {
                request_id,
                timestamp: Utc::now(),
            },
            error: None,
        }
    }

    pub fn error(code: String, message: String, request_id: String, field: Option<String>) -> Self {
        Self {
            data: None,
            meta: ApiMeta {
                request_id,
                timestamp: Utc::now(),
            },
            error: Some(ApiError {
                code,
                message,
                field,
            }),
        }
    }

    pub fn error_with_field(
        code: String,
        message: String,
        request_id: String,
        field: String,
    ) -> Self {
        Self::error(code, message, request_id, Some(field))
    }
}

impl<T> ApiResponse<T> {
    pub fn from_result(result: Result<T, ApiError>, request_id: String) -> Self {
        match result {
            Ok(data) => Self::success(data, request_id),
            Err(error) => Self {
                data: None,
                meta: ApiMeta {
                    request_id,
                    timestamp: Utc::now(),
                },
                error: Some(error),
            },
        }
    }
}

// Common error types
pub const VALIDATION_ERROR: &str = "VALIDATION_ERROR";
pub const UNAUTHORIZED_ERROR: &str = "UNAUTHORIZED";
pub const INVALID_CREDENTIALS: &str = "INVALID_CREDENTIALS";
pub const USER_ALREADY_EXISTS: &str = "USER_ALREADY_EXISTS";
pub const USER_NOT_FOUND: &str = "USER_NOT_FOUND";
pub const FORBIDDEN_ERROR: &str = "FORBIDDEN";
pub const NOT_FOUND_ERROR: &str = "NOT_FOUND";
pub const INTERNAL_ERROR: &str = "INTERNAL_ERROR";
pub const DATABASE_ERROR: &str = "DATABASE_ERROR";
pub const CONFIGURATION_ERROR: &str = "CONFIGURATION_ERROR";

pub fn create_error(code: &str, message: &str, field: Option<String>) -> ApiError {
    ApiError {
        code: code.to_string(),
        message: message.to_string(),
        field,
    }
}

pub fn create_validation_error(message: &str) -> ApiError {
    create_error(VALIDATION_ERROR, message, None)
}

pub fn create_unauthorized_error() -> ApiError {
    create_error(UNAUTHORIZED_ERROR, "Unauthorized", None)
}

pub fn create_invalid_credentials_error() -> ApiError {
    create_error(INVALID_CREDENTIALS, "Invalid credentials", None)
}

pub fn create_user_already_exists_error() -> ApiError {
    create_error(USER_ALREADY_EXISTS, "User already exists", None)
}

pub fn create_user_not_found_error() -> ApiError {
    create_error(USER_NOT_FOUND, "User not found", None)
}

pub fn create_forbidden_error() -> ApiError {
    create_error(FORBIDDEN_ERROR, "Forbidden", None)
}

pub fn create_not_found_error() -> ApiError {
    create_error(NOT_FOUND_ERROR, "Resource not found", None)
}

pub fn create_internal_error() -> ApiError {
    create_error(INTERNAL_ERROR, "Internal server error", None)
}

pub fn create_database_error(field: String) -> ApiError {
    create_error(DATABASE_ERROR, "Database error", Some(field))
}

pub fn create_configuration_error(field: String) -> ApiError {
    create_error(CONFIGURATION_ERROR, "Configuration error", Some(field))
}
