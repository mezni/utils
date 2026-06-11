use actix_web::{HttpResponse, ResponseError};
use serde::Serialize;
use std::fmt;

#[derive(Debug, Serialize)]
pub struct AppError {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl AppError {
    fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            details: None,
        }
    }

    fn with_details(
        code: impl Into<String>,
        message: impl Into<String>,
        details: serde_json::Value,
    ) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            details: Some(details),
        }
    }

    pub fn invalid_event_name(name: &str, allowed: &[&str]) -> Self {
        Self::with_details(
            "INVALID_EVENT_NAME",
            format!("Unknown event name: {}", name),
            serde_json::json!({
                "field": "event_name",
                "allowed": allowed,
            }),
        )
    }

    pub fn missing_session_id() -> Self {
        Self::new(
            "MISSING_SESSION_ID",
            "session_id is required and must be non-empty",
        )
    }

    pub fn invalid_timestamp() -> Self {
        Self::new(
            "INVALID_TIMESTAMP",
            "client_ts must be a valid ISO 8601 timestamp",
        )
    }

    pub fn invalid_payload() -> Self {
        Self::new("INVALID_PAYLOAD", "payload must be a valid JSON object")
    }

    pub fn payload_too_large() -> Self {
        Self::new("PAYLOAD_TOO_LARGE", "Event exceeds maximum size of 64KB")
    }

    pub fn batch_size_exceeded() -> Self {
        Self::new(
            "BATCH_SIZE_EXCEEDED",
            "Batch must contain 1-100 events",
        )
    }

    pub fn batch_too_large() -> Self {
        Self::new("BATCH_TOO_LARGE", "Batch exceeds maximum size of 512KB")
    }

    pub fn invalid_json() -> Self {
        Self::new("INVALID_JSON", "Request body must be valid JSON")
    }

    pub fn unsupported_media_type() -> Self {
        Self::new(
            "UNSUPPORTED_MEDIA_TYPE",
            "Content-Type must be application/json",
        )
    }

    pub fn db_disconnected() -> Self {
        Self::new("DB_DISCONNECTED", "Database is unreachable")
    }

    pub fn rate_limited() -> Self {
        Self::new("RATE_LIMITED", "Too many requests from this IP")
    }

    pub fn db_error(msg: impl Into<String>) -> Self {
        Self::new("DB_ERROR", msg.into())
    }
}

impl ResponseError for AppError {
    fn error_response(&self) -> HttpResponse {
        let status = self.status_code();
        let body = serde_json::json!({
            "data": null,
            "error": self,
            "meta": {}
        });
        HttpResponse::build(status).json(body)
    }

    fn status_code(&self) -> actix_web::http::StatusCode {
        match self.code.as_str() {
            "INVALID_EVENT_NAME"
            | "MISSING_SESSION_ID"
            | "INVALID_TIMESTAMP"
            | "INVALID_PAYLOAD"
            | "BATCH_SIZE_EXCEEDED" => actix_web::http::StatusCode::UNPROCESSABLE_ENTITY,
            "PAYLOAD_TOO_LARGE" | "BATCH_TOO_LARGE" => {
                actix_web::http::StatusCode::PAYLOAD_TOO_LARGE
            }
            "UNSUPPORTED_MEDIA_TYPE" => actix_web::http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "RATE_LIMITED" => actix_web::http::StatusCode::TOO_MANY_REQUESTS,
            "DB_DISCONNECTED" => actix_web::http::StatusCode::SERVICE_UNAVAILABLE,
            _ => actix_web::http::StatusCode::BAD_REQUEST,
        }
    }
}
