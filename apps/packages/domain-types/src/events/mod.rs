//! Event types and schemas for telemetry ingestion

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Fixed enum for event type governance
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventType {
    AUTH_LOGIN,
    AUTH_LOGOUT,
    TOKEN_REFRESH,
    LOCATION_UPDATE,
    SESSION_START,
    SESSION_END,
    DRIVER_STATUS,
    INVENTORY_UPDATE,
    PRICE_CHANGE,
    STOCK_ALERT,
    ERROR_UNHANDLED,
    FAVORITE_ADDED,
    FAVORITE_REMOVED,
    SEARCH_EXECUTED,
    SEARCH_SELECTED,
    FILTER_CHANGED,
    OFFLINE_MODE_ENTERED,
}

impl EventType {
    /// Convert string to EventType, validating against known values
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "AUTH_LOGIN" => Ok(EventType::AUTH_LOGIN),
            "AUTH_LOGOUT" => Ok(EventType::AUTH_LOGOUT),
            "TOKEN_REFRESH" => Ok(EventType::TOKEN_REFRESH),
            "LOCATION_UPDATE" => Ok(EventType::LOCATION_UPDATE),
            "SESSION_START" => Ok(EventType::SESSION_START),
            "SESSION_END" => Ok(EventType::SESSION_END),
            "DRIVER_STATUS" => Ok(EventType::DRIVER_STATUS),
            "INVENTORY_UPDATE" => Ok(EventType::INVENTORY_UPDATE),
            "PRICE_CHANGE" => Ok(EventType::PRICE_CHANGE),
            "STOCK_ALERT" => Ok(EventType::STOCK_ALERT),
            "ERROR_UNHANDLED" => Ok(EventType::ERROR_UNHANDLED),
            "FAVORITE_ADDED" => Ok(EventType::FAVORITE_ADDED),
            "FAVORITE_REMOVED" => Ok(EventType::FAVORITE_REMOVED),
            "SEARCH_EXECUTED" => Ok(EventType::SEARCH_EXECUTED),
            "SEARCH_SELECTED" => Ok(EventType::SEARCH_SELECTED),
            "FILTER_CHANGED" => Ok(EventType::FILTER_CHANGED),
            "OFFLINE_MODE_ENTERED" => Ok(EventType::OFFLINE_MODE_ENTERED),
            _ => Err(format!("Unknown event type: {}", s)),
        }
    }

    /// Get all valid event type strings
    pub fn all() -> &'static [&'static str] {
        &[
            "AUTH_LOGIN",
            "AUTH_LOGOUT",
            "TOKEN_REFRESH",
            "LOCATION_UPDATE",
            "SESSION_START",
            "SESSION_END",
            "DRIVER_STATUS",
            "INVENTORY_UPDATE",
            "PRICE_CHANGE",
            "STOCK_ALERT",
            "ERROR_UNHANDLED",
            "FAVORITE_ADDED",
            "FAVORITE_REMOVED",
            "SEARCH_EXECUTED",
            "SEARCH_SELECTED",
            "FILTER_CHANGED",
            "OFFLINE_MODE_ENTERED",
        ]
    }
}

/// Location source provenance tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LocationSource {
    /// Location from event payload
    EventLocation,
    /// Location from active session
    SessionLocation,
    /// Location from cached user profile
    LastKnownLocation,
    /// Default location when no location is available
    DefaultLocation,
}

impl LocationSource {
    /// Convert string to LocationSource, validating against known values
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "EVENT_LOCATION" => Ok(LocationSource::EventLocation),
            "SESSION_LOCATION" => Ok(LocationSource::SessionLocation),
            "LAST_KNOWN_LOCATION" => Ok(LocationSource::LastKnownLocation),
            "DEFAULT_LOCATION" => Ok(LocationSource::DefaultLocation),
            _ => Err(format!("Unknown location source: {}", s)),
        }
    }
}

/// Core telemetry event structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryEvent {
    /// Schema version (e.g., "1.0.0")
    pub schema_version: String,
    /// Event type (fixed enum value)
    pub event_type: EventType,
    /// Unique event identifier (UUID v7)
    pub event_id: Uuid,
    /// User who triggered the event (UUID)
    pub user_id: Uuid,
    /// Event timestamp (ISO 8601 format)
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Event payload (flexible JSON)
    pub payload: serde_json::Value,
    /// Unique idempotency key (UUID v7)
    pub idempotency_key: Uuid,
    /// Enriched metadata (location, session, role, system)
    pub enriched_metadata: EnrichedMetadata,
    /// Status of the event
    pub status: TelemetryStatus,
}

/// Status of the telemetry event
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryStatus {
    Pending,
    Processed,
    Failed,
}

/// Enriched metadata attached to events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichedMetadata {
    /// Location metadata with provenance
    pub location: LocationMetadata,
    /// Session context
    pub session: SessionMetadata,
    /// Role context from JWT claims
    pub role: RoleMetadata,
    /// System context
    pub system: SystemMetadata,
}

/// Location metadata with provenance tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocationMetadata {
    /// Latitude coordinate
    pub latitude: Option<f64>,
    /// Longitude coordinate
    pub longitude: Option<f64>,
    /// Country code
    pub country: Option<String>,
    /// City name
    pub city: Option<String>,
    /// Source of location data
    pub location_source: LocationSource,
}

/// Session context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionMetadata {
    /// Session start timestamp
    pub session_start: chrono::DateTime<chrono::Utc>,
    /// Session duration in seconds
    pub session_duration: i64,
    /// Timestamp of last activity in session
    pub last_activity: chrono::DateTime<chrono::Utc>,
}

/// Role context from JWT claims
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleMetadata {
    /// Role from JWT claims (driver, partner, admin)
    pub role: String,
}

/// System context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemMetadata {
    /// Source service name
    pub service_name: String,
    /// Event source identifier
    pub event_source: String,
}

impl TelemetryEvent {
    /// Create a new telemetry event
    pub fn new(
        schema_version: String,
        event_type: EventType,
        event_id: Uuid,
        user_id: Uuid,
        timestamp: chrono::DateTime<chrono::Utc>,
        payload: serde_json::Value,
        enriched_metadata: EnrichedMetadata,
    ) -> Self {
        Self {
            schema_version,
            event_type,
            event_id,
            user_id,
            timestamp,
            payload,
            idempotency_key: Uuid::new_v4(),
            enriched_metadata,
            status: TelemetryStatus::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_type_from_str() {
        assert_eq!(
            EventType::from_str("AUTH_LOGIN"),
            Ok(EventType::AUTH_LOGIN)
        );
        assert_eq!(
            EventType::from_str("AUTH_LOGOUT"),
            Ok(EventType::AUTH_LOGOUT)
        );
        assert_eq!(
            EventType::from_str("LOCATION_UPDATE"),
            Ok(EventType::LOCATION_UPDATE)
        );
        assert_eq!(
            EventType::from_str("UNKNOWN_TYPE"),
            Err("Unknown event type: UNKNOWN_TYPE".to_string())
        );
    }

    #[test]
    fn test_location_source_from_str() {
        assert_eq!(
            LocationSource::from_str("EVENT_LOCATION"),
            Ok(LocationSource::EventLocation)
        );
        assert_eq!(
            LocationSource::from_str("SESSION_LOCATION"),
            Ok(LocationSource::SessionLocation)
        );
        assert_eq!(
            LocationSource::from_str("LAST_KNOWN_LOCATION"),
            Ok(LocationSource::LastKnownLocation)
        );
        assert_eq!(
            LocationSource::from_str("DEFAULT_LOCATION"),
            Ok(LocationSource::DefaultLocation)
        );
        assert_eq!(
            LocationSource::from_str("UNKNOWN_SOURCE"),
            Err("Unknown location source: UNKNOWN_SOURCE".to_string())
        );
    }

    #[test]
    fn test_event_type_serialization() {
        let event = TelemetryEvent::new(
            "1.0.0".to_string(),
            EventType::AUTH_LOGIN,
            Uuid::new_v4(),
            Uuid::new_v4(),
            chrono::Utc::now(),
            serde_json::json!({}),
            EnrichedMetadata {
                location: LocationMetadata {
                    latitude: Some(37.7749),
                    longitude: Some(-122.4194),
                    country: Some("US".to_string()),
                    city: Some("San Francisco".to_string()),
                    location_source: LocationSource::EventLocation,
                },
                session: SessionMetadata {
                    session_start: chrono::Utc::now(),
                    session_duration: 3600,
                    last_activity: chrono::Utc::now(),
                },
                role: RoleMetadata {
                    role: "driver".to_string(),
                },
                system: SystemMetadata {
                    service_name: "auth-service".to_string(),
                    event_source: "AUTH_LOGIN".to_string(),
                },
            },
        );

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"AUTH_LOGIN\""));
        assert!(json.contains("\"EVENT_LOCATION\""));
    }
}
