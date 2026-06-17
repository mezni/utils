use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Charger {
    pub id: String,
    pub connector_type: String,
    pub connector_count: Option<i32>,
    pub power_kw: f64,
    pub status: String,
}
