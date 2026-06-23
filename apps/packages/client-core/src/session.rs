use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// UI state captured for session continuity
/// Does NOT include authentication tokens — Keycloak manages auth independently
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionState {
    pub map_region: Option<MapRegion>,
    pub filters: Option<SessionFilters>,
    pub last_section: Option<String>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapRegion {
    pub latitude: f64,
    pub longitude: f64,
    pub latitude_delta: f64,
    pub longitude_delta: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionFilters {
    pub connector_type: Option<String>,
    pub available_only: Option<bool>,
}

impl SessionState {
    pub fn new() -> Self {
        Self {
            map_region: None,
            filters: None,
            last_section: None,
            timestamp: Utc::now(),
        }
    }

    pub fn is_expired(&self, max_age_minutes: i64) -> bool {
        let elapsed = Utc::now() - self.timestamp;
        elapsed.num_minutes() > max_age_minutes
    }
}

impl Default for SessionState {
    fn default() -> Self {
        Self::new()
    }
}
