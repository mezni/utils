use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProximityQuery {
    pub longitude: f64,
    pub latitude: f64,
    #[serde(default = "default_search_radius")]
    pub search_radius_meters: Option<f64>,
}

fn default_search_radius() -> Option<f64> {
    Some(5000.0)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProximityResponse {
    pub stations: Vec<services_shared::domain::NearbyStationRow>,
    pub count: usize,
}
