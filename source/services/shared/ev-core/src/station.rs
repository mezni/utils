use serde::{Deserialize, Serialize};
use crate::charger::{Charger, CreateChargerRequest};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Station {
    pub id: String,
    pub name: String,
    pub address: String,
    pub lat: f64,
    pub lng: f64,
    pub status: String,
    pub opening_hours: Option<String>,
    pub partner_id: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub deleted_at: Option<chrono::NaiveDateTime>,
    pub chargers: Option<Vec<Charger>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateStationRequest {
    pub name: String,
    pub address: String,
    pub lat: f64,
    pub lng: f64,
    pub partner_id: String,
    pub opening_hours: Option<String>,
    pub chargers: Vec<CreateChargerRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateStationRequest {
    pub name: Option<String>,
    pub address: Option<String>,
    pub lat: Option<f64>,
    pub lng: Option<f64>,
    pub status: Option<String>,
    pub opening_hours: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NearbyStation {
    pub id: String,
    pub name: String,
    pub address: String,
    pub lat: f64,
    pub lng: f64,
    pub status: String,
    pub opening_hours: Option<String>,
    pub partner_id: String,
    pub distance_km: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginatedResponse<T> {
    pub data: Vec<T>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
}
