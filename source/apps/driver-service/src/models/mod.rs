use serde::Serialize;
use sqlx::FromRow;

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

#[derive(Serialize, FromRow)]
pub struct StationSummary {
    pub id: String,
    pub name: String,
    pub address: Option<String>,
    pub latitude: f64,
    pub longitude: f64,
    pub availability_status: Option<String>,
}

#[derive(Serialize, FromRow)]
pub struct StationNearby {
    pub id: String,
    pub name: String,
    pub address: Option<String>,
    pub latitude: f64,
    pub longitude: f64,
    pub availability_status: Option<String>,
    pub distance_meters: f64,
}

#[derive(Serialize)]
pub struct ChargerInfo {
    pub id: String,
    pub connector_type: String,
    pub power_kw: f64,
    pub status: String,
}

#[derive(Serialize)]
pub struct StationDetail {
    pub id: String,
    pub name: String,
    pub address: Option<String>,
    pub latitude: f64,
    pub longitude: f64,
    pub chargers: Vec<ChargerInfo>,
}

#[derive(Serialize)]
pub struct ReviewsStubResponse {
    pub station_id: String,
    pub message: String,
}
