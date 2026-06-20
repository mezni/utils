use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct Charger {
    pub id: String,
    pub station_id: String,
    pub connector_type_id: i64,
    pub current_type_id: i64,
    pub status_id: i64,
    pub power_kw: Option<f64>,
    pub voltage: Option<i32>,
    pub amperage: Option<i32>,
    pub count_available: i32,
    pub count_total: i32,
    pub deleted_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct CreateChargerRequest {
    pub station_id: String,
    pub connector_type_id: i64,
    pub current_type_id: i64,
    pub status_id: i64,
    pub power_kw: Option<f64>,
    pub voltage: Option<i32>,
    pub amperage: Option<i32>,
    pub count_available: Option<i32>,
    pub count_total: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateChargerRequest {
    pub connector_type_id: Option<i64>,
    pub current_type_id: Option<i64>,
    pub status_id: Option<i64>,
    pub power_kw: Option<f64>,
    pub voltage: Option<i32>,
    pub amperage: Option<i32>,
    pub count_available: Option<i32>,
    pub count_total: Option<i32>,
}
