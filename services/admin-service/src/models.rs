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

/// Partner request
#[derive(Debug, Deserialize)]
pub struct PartnerRequest {
    pub name: String,
    pub email: String,
    pub phone: String,
    pub address: String,
}

/// Partner response
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct PartnerResponse {
    pub id: String,
    pub name: String,
    pub email: String,
    pub phone: String,
    pub address: String,
}

/// Partner list response
#[derive(Debug, Serialize)]
pub struct PartnerListResponse {
    pub partners: Vec<PartnerResponse>,
    pub pagination: Option<Pagination>,
}

/// Station request
#[derive(Debug, Deserialize)]
pub struct StationRequest {
    pub partner_id: String,
    pub name: String,
    pub latitude: f64,
    pub longitude: f64,
    pub address: String,
}

/// Station response
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct StationResponse {
    pub id: String,
    pub partner_id: String,
    pub name: String,
    pub latitude: f64,
    pub longitude: f64,
    pub address: String,
}

/// Station list response
#[derive(Debug, Serialize)]
pub struct StationListResponse {
    pub stations: Vec<StationResponse>,
    pub pagination: Option<Pagination>,
}

/// Charger request
#[derive(Debug, Deserialize)]
pub struct ChargerRequest {
    pub station_id: String,
    pub connector_type: String,
    pub power_kw: f64,
    pub status: String,
}

/// Charger response
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct ChargerResponse {
    pub id: String,
    pub station_id: String,
    pub connector_type: String,
    pub power_kw: f64,
    pub status: String,
}

/// Charger list response
#[derive(Debug, Serialize)]
pub struct ChargerListResponse {
    pub chargers: Vec<ChargerResponse>,
    pub pagination: Option<Pagination>,
}

/// Pagination metadata
#[derive(Debug, Serialize)]
pub struct Pagination {
    pub page: u32,
    pub page_size: u32,
    pub total_pages: u32,
    pub total_items: u32,
}
