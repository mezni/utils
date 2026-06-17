use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Station {
    pub id: String,
    pub name: String,
    pub visibility: String,
    pub location: serde_json::Value,
    pub distance_km: f64,
    pub address: Option<String>,
    pub city: String,
    pub connector_types: Option<Vec<String>>,
    pub connector_power: Option<Vec<f64>>,
}

impl From<(String, String, String, serde_json::Value, f64, Option<String>, String, Option<Vec<String>>, Option<Vec<f64>>)> for Station {
    fn from(
        (id, name, visibility, location, distance_km, address, city, connector_types, connector_power): (
            String,
            String,
            String,
            serde_json::Value,
            f64,
            Option<String>,
            String,
            Option<Vec<String>>,
            Option<Vec<f64>>,
        ),
    ) -> Self {
        Station {
            id,
            name,
            visibility,
            location,
            distance_km,
            address,
            city,
            connector_types,
            connector_power,
        }
    }
}
