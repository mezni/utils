use serde::{Deserialize, Serialize};
use super::nanoid::generate_nanoid;

const PREFIX: &str = "STA";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Station {
    pub station_id: String,
    pub osm_id: Option<i64>,
    pub partner_id: Option<String>,
    pub name: String,
    pub address: Option<String>,
    pub lat: f64,
    pub lon: f64,
    pub created_by_uuid: Option<uuid::Uuid>,
    pub updated_by_uuid: Option<uuid::Uuid>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
    pub deleted_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl Station {
    pub fn new(name: String, lat: f64, lon: f64) -> Self {
        Self {
            station_id: format!("{}-{}", PREFIX, generate_nanoid()),
            osm_id: None,
            partner_id: None,
            name,
            address: None,
            lat,
            lon,
            created_by_uuid: None,
            updated_by_uuid: None,
            created_at: chrono::Utc::now(),
            updated_at: None,
            deleted_at: None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateStationRequest {
    pub name: String,
    pub lat: f64,
    pub lon: f64,
    pub osm_id: Option<i64>,
    pub partner_id: Option<String>,
    pub address: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateStationRequest {
    pub name: Option<String>,
    pub address: Option<String>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub partner_id: Option<String>,
}

pub fn validate_lat(lat: f64) -> bool {
    (-90.0..=90.0).contains(&lat)
}

pub fn validate_lon(lon: f64) -> bool {
    (-180.0..=180.0).contains(&lon)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_station_id_format() {
        let s = Station::new("Test".into(), 36.8, 10.1);
        assert!(s.station_id.starts_with("STA-"));
        assert_eq!(s.station_id.len(), 16);
    }

    #[test]
    fn test_validate_lat() {
        assert!(validate_lat(0.0));
        assert!(validate_lat(-90.0));
        assert!(validate_lat(90.0));
        assert!(!validate_lat(-90.1));
        assert!(!validate_lat(90.1));
    }

    #[test]
    fn test_validate_lon() {
        assert!(validate_lon(0.0));
        assert!(validate_lon(-180.0));
        assert!(validate_lon(180.0));
        assert!(!validate_lon(-180.1));
        assert!(!validate_lon(180.1));
    }
}
