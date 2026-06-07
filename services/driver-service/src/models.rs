// Data models module
use serde::{Deserialize, Serialize};

/// Health check request (no body needed)
#[derive(Debug, Deserialize)]
pub struct HealthCheckRequest {}

/// Health check response
#[derive(Debug, Serialize)]
pub struct HealthCheckResponse {
    pub status: String,
    pub service: String,
    pub db: String,
}

/// Nearby stations request
#[derive(Debug, Deserialize)]
pub struct NearbyStationsRequest {
    pub lat: f64,
    pub lng: f64,
    pub radius_km: f64,
}

/// Station response
#[derive(Debug, Serialize)]
pub struct StationResponse {
    pub id: String,
    pub name: String,
    pub latitude: f64,
    pub longitude: f64,
    pub distance_km: f64,
}

/// Nearby stations response
#[derive(Debug, Serialize)]
pub struct NearbyStationsResponse {
    pub stations: Vec<StationResponse>,
}
