use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Charger {
    pub id: String,
    pub station_id: String,
    #[serde(rename = "type")]
    pub charger_type: String,
    pub power_kw: f64,
    pub status: String,
    pub price_per_kwh: f64,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub deleted_at: Option<chrono::NaiveDateTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateChargerRequest {
    #[serde(rename = "type")]
    pub charger_type: String,
    pub power_kw: f64,
    pub price_per_kwh: Option<f64>,
}
