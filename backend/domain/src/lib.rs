use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Charger {
    pub id: String,
    pub plug_type: String,
    pub power_output: u32,
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StationHub {
    pub id: String,
    pub name: String,
    pub provider_id: String,
    pub provider_name: String,
    pub latitude: f64,
    pub longitude: f64,
    pub status: String,
    pub chargers: Vec<Charger>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Provider {
    pub id: String,
    pub name: String,
}
