//! Event enrichment middleware for telemetry ingestion

use domain_types::events::{LocationMetadata, LocationSource, RoleMetadata, SessionMetadata, SystemMetadata};
use chrono::{Duration, Utc};
use tracing::{debug, info, warn};

/// Enrich event with location, session, role, and system metadata
pub fn enrich_event(
    user_id: uuid::Uuid,
    role: Option<String>,
    service_name: &str,
    event_source: &str,
) -> (RoleMetadata, SessionMetadata, SystemMetadata) {
    // Role context from JWT claims
    let role_metadata = RoleMetadata {
        role: role.unwrap_or_else(|| "unknown".to_string()),
    };

    // Session metadata (simplified - in production, this would come from session storage)
    let session_start = Utc::now() - Duration::seconds(1800); // 30 minutes ago
    let session_duration = 1800; // 30 minutes
    let last_activity = Utc::now();
    let session_metadata = SessionMetadata {
        session_start,
        session_duration,
        last_activity,
    };

    // System context
    let system_metadata = SystemMetadata {
        service_name: service_name.to_string(),
        event_source: event_source.to_string(),
    };

    (role_metadata, session_metadata, system_metadata)
}

/// Enrich event with location provenance
///
/// Location provenance determines the source of location data:
/// - EventLocation: Location from event payload
/// - SessionLocation: Location from active session
/// - LastKnownLocation: Cached location from user profile
/// - DefaultLocation: Fallback when no location is available
pub fn enrich_location(
    location: Option<f64>,
    location_source_str: &str,
) -> LocationMetadata {
    let location_source = match location_source_str.to_lowercase().as_str() {
        "event_location" => LocationSource::EventLocation,
        "session_location" => LocationSource::SessionLocation,
        "last_known_location" => LocationSource::LastKnownLocation,
        "default_location" | "default" | "unknown" | "none" => LocationSource::DefaultLocation,
        _ => {
            warn!("Unknown location source: {}, using DefaultLocation", location_source_str);
            LocationSource::DefaultLocation
        }
    };

    // Parse latitude and longitude
    let (latitude, longitude) = if location.is_some() {
        // Simplified - in production, this would parse from full location data
        let lat_lon: Vec<f64> = location.unwrap().to_string()
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();

        if lat_lon.len() >= 2 {
            (lat_lon[0], lat_lon[1])
        } else {
            (None, None)
        }
    } else {
        (None, None)
    };

    LocationMetadata {
        latitude,
        longitude,
        country: None,
        city: None,
        location_source,
    }
}

/// Create location metadata with explicit location source
pub fn create_location_metadata(
    latitude: Option<f64>,
    longitude: Option<f64>,
    location_source: LocationSource,
) -> LocationMetadata {
    LocationMetadata {
        latitude,
        longitude,
        country: None,
        city: None,
        location_source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enrich_event() {
        let (role, session, system) = enrich_event(
            Uuid::new_v4(),
            Some("driver".to_string()),
            "auth-service",
            "AUTH_LOGIN",
        );

        assert_eq!(role.role, "driver");
        assert!(session.session_start < Utc::now());
        assert_eq!(system.service_name, "auth-service");
    }

    #[test]
    fn test_enrich_location_event_location() {
        let loc = enrich_location(Some(37.7749), "event_location");
        assert_eq!(loc.location_source, LocationSource::EventLocation);
        assert_eq!(loc.latitude, Some(37.7749));
    }

    #[test]
    fn test_enrich_location_session_location() {
        let loc = enrich_location(Some(37.7749), "session_location");
        assert_eq!(loc.location_source, LocationSource::SessionLocation);
    }

    #[test]
    fn test_enrich_location_last_known_location() {
        let loc = enrich_location(Some(37.7749), "last_known_location");
        assert_eq!(loc.location_source, LocationSource::LastKnownLocation);
    }

    #[test]
    fn test_enrich_location_default_location() {
        let loc = enrich_location(None, "default_location");
        assert_eq!(loc.location_source, LocationSource::DefaultLocation);
        assert!(loc.latitude.is_none());
    }

    #[test]
    fn test_create_location_metadata() {
        let loc = create_location_metadata(
            Some(37.7749),
            Some(-122.4194),
            LocationSource::EventLocation,
        );

        assert_eq!(loc.location_source, LocationSource::EventLocation);
        assert_eq!(loc.latitude, Some(37.7749));
        assert_eq!(loc.longitude, Some(-122.4194));
    }

    #[test]
    fn test_enrich_location_unknown_source() {
        let loc = enrich_location(Some(37.7749), "unknown_source");
        assert_eq!(loc.location_source, LocationSource::DefaultLocation);
    }
}
