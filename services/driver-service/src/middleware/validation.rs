//! Event validation middleware for telemetry ingestion

use crate::domain_types::events::{EventType, LocationSource, TelemetryEvent};
use crate::middleware::idempotency::is_valid_idempotency_key;
use crate::middleware::telemetry::DeadLetterError;
use chrono::DateTime;
use sqlx::postgres::PgPool;
use thiserror::Error;
use tracing::{debug, error, info, warn};

/// Validation error types
#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("Schema version missing or invalid: {0}")]
    InvalidSchemaVersion(String),

    #[error("Unknown event type: {0}")]
    UnknownEventType(String),

    #[error("Invalid user_id: {0}")]
    InvalidUserId(String),

    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(String),

    #[error("Payload is not a valid JSON object")]
    InvalidPayload,

    #[error("Idempotency key missing or invalid")]
    InvalidIdempotencyKey(String),

    #[error("Location source missing or invalid: {0}")]
    InvalidLocationSource(String),
}

/// Validation result
pub type ValidationResult = Result<TelemetryEvent, ValidationError>;

/// Validate event against schema version governance
/// Reject unknown versions and deprecated versions (> 30 days old)
pub fn validate_schema_version(version: &str) -> Result<(), ValidationError> {
    // Schema version "1.0.0" is the only valid version
    // (Future versions can be added as new event schemas are released)
    if version != "1.0.0" {
        return Err(ValidationError::InvalidSchemaVersion(format!(
            "Unknown schema version: {}. Valid versions: 1.0.0",
            version
        )));
    }

    Ok(())
}

/// Validate event_type against enum
pub fn validate_event_type(event_type: &str) -> Result<EventType, ValidationError> {
    EventType::from_str(event_type)
        .map_err(|err| ValidationError::UnknownEventType(err))
}

/// Validate user_id as UUID
pub fn validate_user_id(user_id: &str) -> Result<(), ValidationError> {
    uuid::Uuid::parse_str(user_id)
        .map(|_| ())
        .map_err(|_| ValidationError::InvalidUserId(user_id.to_string()))
}

/// Validate timestamp as ISO 8601 format
pub fn validate_timestamp(timestamp: &str) -> Result<DateTime<chrono::Utc>, ValidationError> {
    DateTime::parse_from_rfc3339(timestamp)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .map_err(|_| ValidationError::InvalidTimestamp(timestamp.to_string()))
}

/// Validate payload as valid JSON object
pub fn validate_payload(payload: &serde_json::Value) -> Result<(), ValidationError> {
    if !payload.is_object() {
        return Err(ValidationError::InvalidPayload);
    }
    Ok(())
}

/// Validate idempotency key
pub fn validate_idempotency_key(key: &str) -> Result<(), ValidationError> {
    if !is_valid_idempotency_key(key) {
        return Err(ValidationError::InvalidIdempotencyKey(key.to_string()));
    }
    Ok(())
}

/// Validate location source against enum
pub fn validate_location_source(location_source: &str) -> Result<LocationSource, ValidationError> {
    LocationSource::from_str(location_source)
        .map_err(|err| ValidationError::InvalidLocationSource(err))
}

/// Complete validation pipeline for telemetry event
pub fn validate_event(event: &TelemetryEvent) -> Result<(), ValidationError> {
    validate_schema_version(&event.schema_version)?;
    validate_event_type(&format!("{:?}", event.event_type))?;
    validate_user_id(&event.user_id.to_string())?;
    validate_timestamp(&event.timestamp.to_rfc3339())?;
    validate_payload(&event.payload)?;
    validate_idempotency_key(&event.idempotency_key.to_string())?;
    validate_location_source(&format!("{:?}", event.enriched_metadata.location.location_source))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;

    fn create_test_event() -> TelemetryEvent {
        TelemetryEvent {
            schema_version: "1.0.0".to_string(),
            event_type: EventType::AUTH_LOGIN,
            event_id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: json!({"login_method": "password"}),
            idempotency_key: Uuid::new_v7(),
            enriched_metadata: crate::domain_types::events::EnrichedMetadata {
                location: crate::domain_types::events::LocationMetadata {
                    latitude: Some(37.7749),
                    longitude: Some(-122.4194),
                    country: Some("US".to_string()),
                    city: Some("San Francisco".to_string()),
                    location_source: LocationSource::EventLocation,
                },
                session: crate::domain_types::events::SessionMetadata {
                    session_start: Utc::now(),
                    session_duration: 3600,
                    last_activity: Utc::now(),
                },
                role: crate::domain_types::events::RoleMetadata {
                    role: "driver".to_string(),
                },
                system: crate::domain_types::events::SystemMetadata {
                    service_name: "auth-service".to_string(),
                    event_source: "AUTH_LOGIN".to_string(),
                },
            },
            status: crate::domain_types::events::TelemetryStatus::Pending,
        }
    }

    #[test]
    fn test_validate_schema_version_valid() {
        assert!(validate_schema_version("1.0.0").is_ok());
    }

    #[test]
    fn test_validate_schema_version_invalid() {
        assert!(validate_schema_version("2.0.0").is_err());
        assert!(validate_schema_version("invalid").is_err());
    }

    #[test]
    fn test_validate_event_type_valid() {
        assert!(validate_event_type("AUTH_LOGIN").is_ok());
    }

    #[test]
    fn test_validate_event_type_invalid() {
        assert!(validate_event_type("INVALID_TYPE").is_err());
    }

    #[test]
    fn test_validate_user_id_valid() {
        assert!(validate_user_id(&Uuid::new_v4().to_string()).is_ok());
    }

    #[test]
    fn test_validate_user_id_invalid() {
        assert!(validate_user_id("invalid-uuid").is_err());
    }

    #[test]
    fn test_validate_timestamp_valid() {
        assert!(validate_timestamp(&Utc::now().to_rfc3339()).is_ok());
    }

    #[test]
    fn test_validate_timestamp_invalid() {
        assert!(validate_timestamp("invalid-date").is_err());
    }

    #[test]
    fn test_validate_payload_valid() {
        let event = create_test_event();
        assert!(validate_payload(&event.payload).is_ok());
    }

    #[test]
    fn test_validate_payload_invalid() {
        assert!(validate_payload(&serde_json::json!(123)).is_err());
        assert!(validate_payload(&serde_json::json!("string")).is_err());
    }

    #[test]
    fn test_validate_event_complete() {
        let event = create_test_event();
        assert!(validate_event(&event).is_ok());
    }
}
