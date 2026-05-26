use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct Charger {
    pub id: String,
    pub station_id: String,
    pub connector_type_id: String,
    pub power_kw: f64,
    pub current_type: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct CreateChargerRequest {
    pub connector_type_id: String,
    pub power_kw: f64,
    pub current_type: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateChargerRequest {
    pub status: Option<String>,
    pub power_kw: Option<f64>,
    pub current_type: Option<String>,
    pub updated_at: DateTime<Utc>,
}
