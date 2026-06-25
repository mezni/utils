use serde::{Deserialize, Serialize};
use super::nanoid::generate_nanoid;

const PREFIX: &str = "CHG";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Charger {
    pub charger_id: String,
    pub station_id: String,
    pub connector_type_id: i32,
    pub status_id: i32,
    pub current_type_id: i32,
    pub power_kw: Option<f64>,
    pub voltage: Option<i32>,
    pub amperage: Option<i32>,
    pub count_available: i32,
    pub count_total: i32,
    pub created_by_uuid: Option<uuid::Uuid>,
    pub updated_by_uuid: Option<uuid::Uuid>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
    pub deleted_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl Charger {
    pub fn new(station_id: String, connector_type_id: i32, status_id: i32, current_type_id: i32) -> Self {
        Self {
            charger_id: format!("{}-{}", PREFIX, generate_nanoid()),
            station_id,
            connector_type_id,
            status_id,
            current_type_id,
            power_kw: None,
            voltage: None,
            amperage: None,
            count_available: 1,
            count_total: 1,
            created_by_uuid: None,
            updated_by_uuid: None,
            created_at: chrono::Utc::now(),
            updated_at: None,
            deleted_at: None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateChargerRequest {
    pub station_id: String,
    pub connector_type_id: i32,
    pub status_id: i32,
    pub current_type_id: i32,
    pub power_kw: Option<f64>,
    pub voltage: Option<i32>,
    pub amperage: Option<i32>,
    pub count_available: Option<i32>,
    pub count_total: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateChargerRequest {
    pub connector_type_id: Option<i32>,
    pub status_id: Option<i32>,
    pub current_type_id: Option<i32>,
    pub power_kw: Option<f64>,
    pub voltage: Option<i32>,
    pub amperage: Option<i32>,
    pub count_available: Option<i32>,
    pub count_total: Option<i32>,
}

pub fn validate_charger_counts(available: i32, total: i32) -> bool {
    available >= 0 && total >= 1 && total >= available
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_charger_id_format() {
        let c = Charger::new("STA-test".into(), 1, 1, 1);
        assert!(c.charger_id.starts_with("CHG-"));
        assert_eq!(c.charger_id.len(), 16);
    }

    #[test]
    fn test_validate_charger_counts() {
        assert!(validate_charger_counts(1, 1));
        assert!(validate_charger_counts(0, 1));
        assert!(validate_charger_counts(2, 5));
        assert!(!validate_charger_counts(-1, 1));
        assert!(!validate_charger_counts(0, 0));
        assert!(!validate_charger_counts(3, 2));
    }
}
