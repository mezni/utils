use serde::{Deserialize, Serialize};
use sqlx::types::Json;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChargerDto {
    pub charger_id: String,
    pub code: String,
    pub plug_type: String,
    pub max_power_kw: i32,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct NearbyStationRow {
    pub station_id: String,
    pub station_name: String,
    pub station_address: Option<String>,
    pub distance_meters: f64,
    pub latitude: f64,
    pub longitude: f64,
    pub available_chargers: Json<Vec<ChargerDto>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformId(String);

impl PlatformId {
    pub fn parse(prefix: &str, input: &str) -> Result<Self, String> {
        let expected_prefix = format!("{}-", prefix);
        if !input.starts_with(&expected_prefix) || input.len() <= expected_prefix.len() {
            return Err(format!(
                "Malformed ID: must start with '{}' followed by identifier",
                expected_prefix
            ));
        }
        Ok(PlatformId(input.to_string()))
    }
}

impl AsRef<str> for PlatformId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
