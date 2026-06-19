use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Charger {
    pub id: String,
    pub station_id: String,
    pub connector_type_id: i32,
    pub status_id: i32,
    pub current_type_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub power_kw: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub voltage: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amperage: Option<i32>,
    pub count_available: i32,
    pub count_total: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub deleted_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateChargerRequest {
    pub station_id: String,
    pub connector_type_id: i32,
    pub status_id: i32,
    pub current_type_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub power_kw: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub voltage: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amperage: Option<i32>,
    pub count_available: i32,
    pub count_total: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateChargerRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub power_kw: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub voltage: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amperage: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count_available: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count_total: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChargerResponse {
    pub id: String,
    pub station_id: String,
    pub connector_type_id: i32,
    pub status_id: i32,
    pub current_type_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub power_kw: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub voltage: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amperage: Option<i32>,
    pub count_available: i32,
    pub count_total: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<Charger> for ChargerResponse {
    fn from(charger: Charger) -> Self {
        Self {
            id: charger.id,
            station_id: charger.station_id,
            connector_type_id: charger.connector_type_id,
            status_id: charger.status_id,
            current_type_id: charger.current_type_id,
            power_kw: charger.power_kw,
            voltage: charger.voltage,
            amperage: charger.amperage,
            count_available: charger.count_available,
            count_total: charger.count_total,
            created_by: charger.created_by,
            updated_by: charger.updated_by,
            created_at: charger.created_at.to_rfc3339(),
            updated_at: charger.updated_at.to_rfc3339(),
        }
    }
}
