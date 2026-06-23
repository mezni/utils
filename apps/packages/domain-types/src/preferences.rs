use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Preferences {
    pub connector_type: Option<String>,
    pub max_distance: Option<i32>,
    pub last_region: Option<Region>,
    pub map_filters: Option<MapFilters>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Region {
    pub lat: f64,
    pub lng: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapFilters {
    pub available_only: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreferencesResponse {
    pub data: Preferences,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdatePreferencesRequest {
    pub connector_type: Option<String>,
    pub max_distance: Option<i32>,
    pub last_region: Option<Region>,
    pub map_filters: Option<MapFilters>,
}
