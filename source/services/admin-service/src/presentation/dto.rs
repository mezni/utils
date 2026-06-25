use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use crate::domain::partner::Partner;
use crate::domain::station::Station;
use crate::domain::charger::Charger;
use crate::domain::errors::ServiceError;

#[derive(Serialize)]
pub struct PartnerResponse {
    pub partner_id: String,
    pub name: String,
    pub partner_type: Option<String>,
    pub support_phone: Option<String>,
    pub support_email: Option<String>,
    pub is_verified: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<Partner> for PartnerResponse {
    fn from(p: Partner) -> Self {
        Self {
            partner_id: p.partner_id,
            name: p.name,
            partner_type: p.partner_type,
            support_phone: p.support_phone,
            support_email: p.support_email,
            is_verified: p.is_verified,
            created_at: p.created_at,
            updated_at: p.updated_at,
        }
    }
}

#[derive(Serialize)]
pub struct StationResponse {
    pub station_id: String,
    pub osm_id: Option<i64>,
    pub partner_id: Option<String>,
    pub name: String,
    pub address: Option<String>,
    pub lat: f64,
    pub lon: f64,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<Station> for StationResponse {
    fn from(s: Station) -> Self {
        Self {
            station_id: s.station_id,
            osm_id: s.osm_id,
            partner_id: s.partner_id,
            name: s.name,
            address: s.address,
            lat: s.lat,
            lon: s.lon,
            created_at: s.created_at,
            updated_at: s.updated_at,
        }
    }
}

#[derive(Serialize)]
pub struct ChargerResponse {
    pub charger_id: String,
    pub station_id: String,
    pub connector_type_id: i32,
    pub status_id: i32,
    pub current_type_id: i32,
    pub power_kw: Option<f64>,
    pub voltage: Option<i32>,
    pub amperage: Option<i32>,
    pub count_available: i32,
    pub count_total: i32,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<Charger> for ChargerResponse {
    fn from(c: Charger) -> Self {
        Self {
            charger_id: c.charger_id,
            station_id: c.station_id,
            connector_type_id: c.connector_type_id,
            status_id: c.status_id,
            current_type_id: c.current_type_id,
            power_kw: c.power_kw,
            voltage: c.voltage,
            amperage: c.amperage,
            count_available: c.count_available,
            count_total: c.count_total,
            created_at: c.created_at,
            updated_at: c.updated_at,
        }
    }
}

#[derive(Serialize)]
pub struct Pagination {
    pub page: i64,
    pub per_page: i64,
    pub total: i64,
    pub total_pages: i64,
}

#[derive(Serialize)]
pub struct PaginatedResponse<T: Serialize> {
    pub data: Vec<T>,
    pub pagination: Pagination,
}

impl<T: Serialize> PaginatedResponse<T> {
    pub fn new(data: Vec<T>, total: i64, page: i64, per_page: i64) -> Self {
        let total_pages = (total as f64 / per_page as f64).ceil() as i64;
        Self {
            data,
            pagination: Pagination { page, per_page, total, total_pages },
        }
    }
}

pub fn error_response(e: ServiceError) -> (StatusCode, Json<Value>) {
    let (code, msg) = match &e {
        ServiceError::NotFound(m) => (StatusCode::NOT_FOUND, m.clone()),
        ServiceError::Validation(m) => (StatusCode::BAD_REQUEST, m.clone()),
        ServiceError::Conflict(m) => (StatusCode::CONFLICT, m.clone()),
        ServiceError::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "internal server error".into()),
    };
    (code, Json(json!({"error": msg})))
}

#[derive(Deserialize)]
pub struct PaginationParams {
    pub page: Option<i64>,
    pub per_page: Option<i64>,
}

impl PaginationParams {
    pub fn page(&self) -> i64 {
        self.page.unwrap_or(1).max(1)
    }

    pub fn per_page(&self) -> i64 {
        self.per_page.unwrap_or(20).clamp(1, 100)
    }
}
