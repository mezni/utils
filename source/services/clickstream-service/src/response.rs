use serde::Serialize;

use crate::errors::AppError;

#[derive(Debug, Serialize)]
pub struct Meta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ApiResponse<T: Serialize> {
    pub data: Option<T>,
    pub error: Option<AppError>,
    pub meta: Meta,
}

impl<T: Serialize> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            data: Some(data),
            error: None,
            meta: Meta { request_id: None },
        }
    }

    pub fn error(err: AppError) -> Self {
        Self {
            data: None,
            error: Some(err),
            meta: Meta { request_id: None },
        }
    }
}
