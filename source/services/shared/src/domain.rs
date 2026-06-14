use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use chrono::{DateTime, Utc};

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
    
    // SQLx automatically deserializes the native JSONB array fields into our typed sub-struct vectors
    pub available_chargers: Json<Vec<ChargerDto>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlatformId(String);

impl PlatformId {
    pub fn parse(prefix: &str, input: &str) -> Result<Self, String> {
        let expected_prefix = format!("{}-", prefix);
        if !input.starts_with(&expected_prefix) || input.len() <= expected_prefix.len() {
            return Err(format!("Malformed ID pattern. Must follow strict '{}' token formats", expected_prefix));
        }
        Ok(PlatformId(input.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartnerDto {
    pub id: String,
    pub name: String,
    pub partner_type: String,
    pub email: String,
    pub phone: String,
    pub verified: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationDto {
    pub id: String,
    pub partner_id: String,
    pub name: String,
    pub address: String,
    pub email: String,
    pub latitude: f64,
    pub longitude: f64,
    pub availability: String,
    pub verified: bool,
    pub is_live: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChargerDetailDto {
    pub id: String,
    pub station_id: String,
    pub identifier_code: String,
    pub plug_type_code: String,
    pub max_power_kw: i32,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
