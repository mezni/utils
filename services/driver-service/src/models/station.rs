use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Station {
    pub id: String,
    pub name: String,
    pub visibility: String,
    pub lat: f64,
    pub lon: f64,
    pub distance_m: f64,
    pub address: Option<String>,
    pub city: String,
    pub connector_types: Option<Vec<String>>,
    pub connector_power: Option<Vec<f64>>,
}
