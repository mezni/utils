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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_response_serializes_envelope() {
        let resp = ApiResponse {
            data: Some(serde_json::json!({"key": "value"})),
            error: None,
            meta: Some(serde_json::json!({"count": 1})),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["data"]["key"], "value");
        assert!(json["error"].is_null());
        assert_eq!(json["meta"]["count"], 1);
    }

    #[test]
    fn api_response_null_error_when_omitted() {
        let resp: ApiResponse<serde_json::Value> = ApiResponse {
            data: None,
            error: None,
            meta: None,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert!(json["data"].is_null());
        assert!(json["error"].is_null());
        assert!(json["meta"].is_null());
    }

    #[test]
    fn error_response_from_app_error() {
        let err = AppError::NotFound("station_x".into());
        let resp = ErrorResponse::from(&err);
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["error"]["code"], "not_found");
        assert!(json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("station_x"));
        assert!(json["data"].is_null());
    }

    #[test]
    fn validation_error_detail_serializes() {
        let detail = ValidationErrorDetail {
            field: "lat".into(),
            message: "must be between -90 and 90".into(),
        };
        let json = serde_json::to_value(&detail).unwrap();
        assert_eq!(json["field"], "lat");
        assert_eq!(json["message"], "must be between -90 and 90");
    }
}
