use serde::{Deserialize, Serialize};
use validator::Validate;

#[derive(Debug, Serialize, Deserialize, Validate)]
pub struct CreateChargerRequest {
    #[validate(length(min = 1, max = 255, message = "Charger name must be between 1 and 255 characters"))]
    pub name: String,
    
    #[serde(default)]
    pub description: Option<String>,
    
    pub station_id: String,
    
    #[validate(length(min = 1, message = "Charger type is required"))]
    pub charger_type: String,
    
    #[validate(range(min = 0.1, message = "Power output must be greater than 0"))]
    pub power_output: f64,
    
    #[validate(range(min = 0.1, message = "Voltage must be greater than 0"))]
    pub voltage: f64,
    
    #[validate(length(min = 1, message = "Current type is required"))]
    pub current_type: String,
    
    pub connector_types: Vec<String>,
    
    #[serde(default = "default_status")]
    pub status: String,
    
    #[serde(default)]
    pub is_public: Option<bool>,
    
    #[serde(default)]
    pub pricing_info: Option<serde_json::Value>,
}

fn default_status() -> String {
    "AVAILABLE".to_string()
}

#[derive(Debug, Serialize, Deserialize, Validate)]
pub struct UpdateChargerRequest {
    #[validate(length(min = 1, max = 255, message = "Charger name must be between 1 and 255 characters"))]
    pub name: Option<String>,
    
    #[serde(default)]
    pub description: Option<String>,
    
    #[serde(default)]
    pub charger_type: Option<String>,
    
    #[serde(default)]
    #[validate(range(min = 0.1, message = "Power output must be greater than 0"))]
    pub power_output: Option<f64>,
    
    #[serde(default)]
    #[validate(range(min = 0.1, message = "Voltage must be greater than 0"))]
    pub voltage: Option<f64>,
    
    #[serde(default)]
    pub current_type: Option<String>,
    
    #[serde(default)]
    pub connector_types: Option<Vec<String>>,
    
    #[serde(default)]
    pub status: Option<String>,
    
    #[serde(default)]
    pub is_public: Option<bool>,
    
    #[serde(default)]
    pub pricing_info: Option<serde_json::Value>,
    
    #[serde(default)]
    pub is_active: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Validate)]
pub struct UpdateChargerStatusRequest {
    #[validate(length(min = 1, message = "Status is required"))]
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChargerResponse {
    pub id: String,
    pub station_id: String,
    pub name: String,
    pub description: Option<String>,
    pub charger_type: String,
    pub power_output: f64,
    pub voltage: f64,
    pub current_type: String,
    pub connector_types: Vec<String>,
    pub status: String,
    pub last_status_update: Option<chrono::DateTime<chrono::Utc>>,
    pub is_public: bool,
    pub pricing_info: Option<serde_json::Value>,
    pub is_active: bool,
    pub version: i32,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<crate::models::Charger> for ChargerResponse {
    fn from(charger: crate::models::Charger) -> Self {
        Self {
            id: charger.base.id,
            station_id: charger.station_id,
            name: charger.name,
            description: charger.description,
            charger_type: charger.charger_type.to_string(),
            power_output: charger.power_output,
            voltage: charger.voltage,
            current_type: charger.current_type.to_string(),
            connector_types: charger.connector_types,
            status: charger.status.to_string(),
            last_status_update: charger.last_status_update,
            is_public: charger.is_public,
            pricing_info: charger.pricing_info,
            is_active: charger.is_active,
            version: charger.version,
            created_at: charger.base.created_at,
            updated_at: charger.base.updated_at,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChargerListResponse {
    pub chargers: Vec<ChargerResponse>,
    pub total: usize,
}