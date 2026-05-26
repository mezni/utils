use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct Station {
    pub id: String,
    pub owner_id: String,
    pub name: String,
    pub address: String,
    pub city: String,
    pub longitude: f64,
    pub latitude: f64,
    pub is_operational: bool,
    pub is_test: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
pub struct CreateStationRequest {
    pub owner_id: String,
    pub name: String,
    pub address: String,
    pub city: String,
    pub longitude: f64,
    pub latitude: f64,
}

#[derive(Debug, Deserialize)]
pub struct UpdateStationRequest {
    pub name: Option<String>,
    pub address: Option<String>,
    pub city: Option<String>,
    pub longitude: Option<f64>,
    pub latitude: Option<f64>,
    pub is_operational: Option<bool>,
    pub updated_at: DateTime<Utc>,
}
