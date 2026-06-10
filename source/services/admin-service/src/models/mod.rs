use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct CreatePartnerRequest {
    pub name: String,
    #[serde(rename = "type")]
    pub partner_type: String,
    #[serde(default)]
    pub is_verified: bool,
    #[serde(default)]
    pub is_live: bool,
    #[serde(default = "default_true")]
    pub is_active: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Deserialize)]
pub struct UpdatePartnerRequest {
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub partner_type: Option<String>,
    pub is_verified: Option<bool>,
    pub is_live: Option<bool>,
    pub is_active: Option<bool>,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct PartnerResponse {
    pub id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub partner_type: String,
    pub is_verified: bool,
    pub is_live: bool,
    pub is_active: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub created_by: String,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub updated_by: String,
}

#[derive(Deserialize)]
pub struct CreateStationRequest {
    pub partner_id: String,
    pub name: String,
    pub address: Option<String>,
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Deserialize)]
pub struct UpdateStationRequest {
    pub name: Option<String>,
    pub address: Option<String>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct StationResponse {
    pub id: String,
    pub partner_id: String,
    pub name: String,
    pub address: Option<String>,
    pub latitude: f64,
    pub longitude: f64,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub created_by: String,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub updated_by: String,
}

#[derive(Deserialize)]
pub struct CreateChargerRequest {
    pub station_id: String,
    pub connector_type: String,
    pub power_kw: f64,
    pub status: Option<String>,
}

#[derive(Deserialize)]
pub struct UpdateChargerRequest {
    pub connector_type: Option<String>,
    pub power_kw: Option<f64>,
    pub status: Option<String>,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct ChargerResponse {
    pub id: String,
    pub station_id: String,
    pub connector_type: String,
    pub power_kw: f64,
    pub status: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub created_by: String,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub updated_by: String,
}

#[derive(Deserialize)]
pub struct CreateAvailabilityRequest {
    pub status: String,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct AvailabilityResponse {
    pub id: String,
    pub station_id: String,
    pub status: String,
    pub updated_by: String,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Deserialize)]
pub struct PaginationParams {
    pub page: Option<u32>,
    pub page_size: Option<u32>,
}

#[derive(Deserialize)]
pub struct StationListParams {
    pub page: Option<u32>,
    pub page_size: Option<u32>,
    pub partner_id: Option<String>,
}

#[derive(Deserialize)]
pub struct ChargerListParams {
    pub page: Option<u32>,
    pub page_size: Option<u32>,
    pub station_id: Option<String>,
}
