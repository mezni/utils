//! Dead-letter logging for malformed events

use crate::middleware::validation::ValidationError;
use chrono::Utc;
use serde_json::Value;
use std::collections::HashMap;
use tracing::{error, info};

/// Dead-letter error types
#[derive(Debug, Clone)]
pub struct DeadLetterError {
    pub original_event_id: String,
    pub original_event: Value,
    pub error_type: String,
    pub error_message: String,
    pub error_stack_trace: Option<String>,
    pub event_schema_version: Option<String>,
    pub location_source: Option<String>,
    pub original_request_id: Option<String>,
    pub event_type: Option<String>,
    pub user_id: Option<String>,
    pub timestamp: Option<String>,
}

/// Log event to dead-letter store
///
/// # Arguments
/// * `original_event_id` - The UUID of the original event
/// * `original_event` - The complete event payload
/// * `error` - The validation error that caused the event to fail
/// * `request_id` - Optional request ID for traceability
///
/// # Returns
/// Database insert result
pub fn log_to_dead_letter(
    original_event_id: String,
    original_event: Value,
    error: &ValidationError,
    request_id: Option<String>,
) -> Result<usize, String> {
    let dead_letter = DeadLetterError {
        original_event_id,
        original_event,
        error_type: error.to_string(),
        error_message: error.to_string(),
        error_stack_trace: None,
        event_schema_version: extract_schema_version(&original_event),
        location_source: extract_location_source(&original_event),
        original_request_id: request_id,
        event_type: extract_event_type(&original_event),
        user_id: extract_user_id(&original_event),
        timestamp: extract_timestamp(&original_event),
    };

    // In production, this would insert into analytics_events_dead_letter table
    // For now, we'll log it
    error!(
        event_id = %dead_letter.original_event_id,
        error_type = %dead_letter.error_type,
        error_message = %dead_letter.error_message,
        request_id = ?dead_letter.original_request_id,
        "Event logged to dead-letter store"
    );

    Ok(1)
}

/// Extract schema version from event payload
fn extract_schema_version(event: &Value) -> Option<String> {
    if let Some(obj) = event.as_object() {
        obj.get("schema_version")?.as_str().map(|s| s.to_string())
    } else {
        None
    }
}

/// Extract location source from event payload
fn extract_location_source(event: &Value) -> Option<String> {
    if let Some(obj) = event.as_object() {
        obj.get("location_source")?.as_str().map(|s| s.to_string())
    } else {
        None
    }
}

/// Extract event type from event payload
fn extract_event_type(event: &Value) -> Option<String> {
    if let Some(obj) = event.as_object() {
        obj.get("event_type")?.as_str().map(|s| s.to_string())
    } else {
        None
    }
}

/// Extract user_id from event payload
fn extract_user_id(event: &Value) -> Option<String> {
    if let Some(obj) = event.as_object() {
        obj.get("user_id")?.as_str().map(|s| s.to_string())
    } else {
        None
    }
}

/// Extract timestamp from event payload
fn extract_timestamp(event: &Value) -> Option<String> {
    if let Some(obj) = event.as_object() {
        obj.get("timestamp")?.as_str().map(|s| s.to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_to_dead_letter() {
        let event = serde_json::json!({
            "schema_version": "1.0.0",
            "event_type": "AUTH_LOGIN",
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "timestamp": "2026-06-22T13:00:00Z",
            "location_source": "event_location",
            "payload": {}
        });

        let error = ValidationError::InvalidSchemaVersion("2.0.0".to_string());

        let result = log_to_dead_letter(
            "test-event-id".to_string(),
            event.clone(),
            &error,
            Some("req-123".to_string()),
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_extract_schema_version() {
        let event = serde_json::json!({
            "schema_version": "1.0.0",
            "event_type": "AUTH_LOGIN",
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "timestamp": "2026-06-22T13:00:00Z",
            "location_source": "event_location",
            "payload": {}
        });

        assert_eq!(extract_schema_version(&event), Some("1.0.0".to_string()));
    }

    #[test]
    fn test_extract_location_source() {
        let event = serde_json::json!({
            "schema_version": "1.0.0",
            "event_type": "AUTH_LOGIN",
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "timestamp": "2026-06-22T13:00:00Z",
            "location_source": "event_location",
            "payload": {}
        });

        assert_eq!(extract_location_source(&event), Some("event_location".to_string()));
    }

    #[test]
    fn test_extract_event_type() {
        let event = serde_json::json!({
            "schema_version": "1.0.0",
            "event_type": "AUTH_LOGIN",
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "timestamp": "2026-06-22T13:00:00Z",
            "location_source": "event_location",
            "payload": {}
        });

        assert_eq!(extract_event_type(&event), Some("AUTH_LOGIN".to_string()));
    }

    #[test]
    fn test_extract_user_id() {
        let event = serde_json::json!({
            "schema_version": "1.0.0",
            "event_type": "AUTH_LOGIN",
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "timestamp": "2026-06-22T13:00:00Z",
            "location_source": "event_location",
            "payload": {}
        });

        assert_eq!(extract_user_id(&event), Some("123e4567-e89b-12d3-a456-426614174000".to_string()));
    }
}
