use serde::{Deserialize, Serialize};
use services_shared::domain::ChargerDto;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationResponse {
    pub station_id: String,
    pub station_name: String,
    pub station_address: Option<String>,
    pub distance_meters: f64,
    pub latitude: f64,
    pub longitude: f64,
    pub available_chargers: Vec<ChargerDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NearbyStationsResponse {
    pub stations: Vec<StationResponse>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NearbyQuery {
    pub longitude: f64,
    pub latitude: f64,
    pub radius: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
}
