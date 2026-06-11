use serde::Serialize;
use serde_json::Value;

use crate::errors::app_error::AppError;

#[derive(Serialize)]
pub struct ApiResponse<T: Serialize> {
    pub data: Option<T>,
    pub error: Option<ApiErrorBody>,
    pub meta: Option<Value>,
}

#[derive(Serialize)]
pub struct ApiErrorBody {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Vec<ValidationErrorDetail>>,
}

#[derive(Serialize)]
pub struct ValidationErrorDetail {
    pub field: String,
    pub message: String,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub data: Option<Value>,
    pub error: ApiErrorBody,
    pub meta: Option<Value>,
}

impl From<&AppError> for ErrorResponse {
    fn from(err: &AppError) -> Self {
        ErrorResponse {
            data: None,
            error: ApiErrorBody {
                code: err.error_code().to_string(),
                message: err.to_string(),
                details: None,
            },
            meta: None,
        }
    }
}
