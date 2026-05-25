use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Charger {
    pub id: String,
    pub station_id: String,
    pub connector_type_id: String,
    pub power_kw: f64,
    pub current_type: String,
    pub status: String,
}
