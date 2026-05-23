use crate::models::{BaseModel, FullModel, ChargerId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Type};
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, Validate)]
pub struct Charger {
    #[serde(flatten)]
    pub base: BaseModel,
    pub station_id: String,
    pub name: String,
    pub description: Option<String>,
    pub charger_type: ChargerType,
    pub power_output_kw: f64,
    pub voltage: Option<f64>,
    pub current: Option<f64>,
    pub connector_types: Vec<ConnectorType>,
    pub status: ChargerStatus,
    pub last_status_update: Option<DateTime<Utc>>,
    pub is_public: bool,
    pub pricing_info: Option<serde_json::Value>,
    pub is_active: bool,
    #[serde(skip)]
    pub deleted_at: Option<DateTime<Utc>>,
    pub version: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type, sqlx::Type)]
#[sqlx(type_name = "charger_type")]
pub enum ChargerType {
    AC,
    DC,
    DCFC, // DC Fast Charger
}

#[derive(Debug, Clone, Serialize, Deserialize, Type, sqlx::Type)]
#[sqlx(type_name = "connector_type")]
pub enum ConnectorType {
    Type1,
    Type2,
    CCS,
    CHAdeMO,
    Tesla,
    Other,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type, sqlx::Type)]
#[sqlx(type_name = "charger_status")]
pub enum ChargerStatus {
    Available,
    Occupied,
    Offline,
    Maintenance,
    Reserved,
    Faulted,
}

impl Charger {
    /// Create a new charger with default values
    pub fn new(
        station_id: String,
        name: String,
        charger_type: ChargerType,
        power_output_kw: f64,
        connector_types: Vec<ConnectorType>,
    ) -> Self {
        let now = Utc::now();
        Self {
            base: BaseModel {
                id: ChargerId::generate_id(),
                created_at: now,
                updated_at: now,
            },
            station_id,
            name,
            description: None,
            charger_type,
            power_output_kw,
            voltage: None,
            current: None,
            connector_types,
            status: ChargerStatus::Available,
            last_status_update: Some(now),
            is_public: true,
            pricing_info: None,
            is_active: true,
            deleted_at: None,
            version: 1,
        }
    }

    /// Create a charger with all fields
    pub fn create(
        station_id: String,
        name: String,
        description: Option<String>,
        charger_type: ChargerType,
        power_output_kw: f64,
        voltage: Option<f64>,
        current: Option<f64>,
        connector_types: Vec<ConnectorType>,
        status: ChargerStatus,
        is_public: bool,
        pricing_info: Option<serde_json::Value>,
    ) -> Self {
        let now = Utc::now();
        Self {
            base: BaseModel {
                id: ChargerId::generate_id(),
                created_at: now,
                updated_at: now,
            },
            station_id,
            name,
            description,
            charger_type,
            power_output_kw,
            voltage,
            current,
            connector_types,
            status,
            last_status_update: Some(now),
            is_public,
            pricing_info,
            is_active: true,
            deleted_at: None,
            version: 1,
        }
    }

    /// Update charger information
    pub fn update(
        &mut self,
        name: Option<String>,
        description: Option<String>,
        charger_type: Option<ChargerType>,
        power_output_kw: Option<f64>,
        voltage: Option<f64>,
        current: Option<f64>,
        connector_types: Option<Vec<ConnectorType>>,
        status: Option<ChargerStatus>,
        is_public: Option<bool>,
        pricing_info: Option<serde_json::Value>,
        is_active: Option<bool>,
    ) {
        let now = Utc::now();
        
        if let Some(name) = name {
            self.name = name;
        }
        if let Some(description) = description {
            self.description = description;
        }
        if let Some(charger_type) = charger_type {
            self.charger_type = charger_type;
        }
        if let Some(power_output_kw) = power_output_kw {
            self.power_output_kw = power_output_kw;
        }
        if let Some(voltage) = voltage {
            self.voltage = voltage;
        }
        if let Some(current) = current {
            self.current = current;
        }
        if let Some(connector_types) = connector_types {
            self.connector_types = connector_types;
        }
        if let Some(status) = status {
            self.status = status;
            self.last_status_update = Some(now);
        }
        if let Some(is_public) = is_public {
            self.is_public = is_public;
        }
        if let Some(pricing_info) = pricing_info {
            self.pricing_info = pricing_info;
        }
        if let Some(is_active) = is_active {
            self.is_active = is_active;
        }
        
        self.base.updated_at = now;
        self.version += 1;
    }

    /// Update charger status
    pub fn update_status(&mut self, status: ChargerStatus) {
        let now = Utc::now();
        self.status = status;
        self.last_status_update = Some(now);
        self.base.updated_at = now;
        self.version += 1;
    }

    /// Soft delete the charger
    pub fn delete(&mut self) {
        self.deleted_at = Some(Utc::now());
        self.base.updated_at = Utc::now();
        self.version += 1;
    }

    /// Restore the charger (undo soft delete)
    pub fn restore(&mut self) {
        self.deleted_at = None;
        self.base.updated_at = Utc::now();
        self.version += 1;
    }

    /// Check if the charger is deleted
    pub fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }

    /// Check if the charger is active (not deleted and is_active flag is true)
    pub fn is_active(&self) -> bool {
        !self.is_deleted() && self.is_active
    }

    /// Check if the charger is available for charging
    pub fn is_available(&self) -> bool {
        self.is_active() && self.status == ChargerStatus::Available
    }

    /// Get the charger ID
    pub fn id(&self) -> &str {
        &self.base.id
    }

    /// Get the charger name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the station ID
    pub fn station_id(&self) -> &str {
        &self.station_id
    }

    /// Get the charger version
    pub fn version(&self) -> i32 {
        self.version
    }

    /// Get the next version number
    pub fn next_version(&self) -> i32 {
        self.version + 1
    }

    /// Get the charger status
    pub fn status(&self) -> &ChargerStatus {
        &self.status
    }

    /// Get the last status update time
    pub fn last_status_update(&self) -> Option<&DateTime<Utc>> {
        self.last_status_update.as_ref()
    }

    /// Validate the charger data
    pub fn validate(&self) -> Result<(), String> {
        // Validate station_id
        if !StationId::validate_id(&self.station_id) {
            return Err("Invalid station ID format".to_string());
        }

        // Validate name
        if self.name.trim().is_empty() {
            return Err("Charger name cannot be empty".to_string());
        }
        if self.name.len() > 255 {
            return Err("Charger name cannot exceed 255 characters".to_string());
        }

        // Validate power output
        if self.power_output_kw <= 0.0 {
            return Err("Power output must be greater than 0".to_string());
        }
        if self.power_output_kw > 1000.0 {
            return Err("Power output cannot exceed 1000 kW".to_string());
        }

        // Validate voltage if provided
        if let Some(voltage) = self.voltage {
            if voltage <= 0.0 {
                return Err("Voltage must be greater than 0".to_string());
            }
            if voltage > 1000.0 {
                return Err("Voltage cannot exceed 1000V".to_string());
            }
        }

        // Validate current if provided
        if let Some(current) = self.current {
            if current <= 0.0 {
                return Err("Current must be greater than 0".to_string());
            }
            if current > 1000.0 {
                return Err("Current cannot exceed 1000A".to_string());
            }
        }

        // Validate connector types
        if self.connector_types.is_empty() {
            return Err("At least one connector type is required".to_string());
        }

        // Validate version
        if self.version < 1 {
            return Err("Version must be greater than or equal to 1".to_string());
        }

        Ok(())
    }
}

impl From<Charger> for FullModel {
    fn from(charger: Charger) -> Self {
        FullModel {
            base: charger.base,
            deleted_at: charger.deleted_at,
            version: charger.version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_new_charger() {
        let station_id = StationId::generate_id();
        let charger = Charger::new(
            station_id.clone(),
            "Test Charger".to_string(),
            ChargerType::AC,
            7.4,
            vec![ConnectorType::Type2],
        );
        
        assert_eq!(charger.station_id, station_id);
        assert_eq!(charger.name, "Test Charger");
        assert_eq!(charger.charger_type, ChargerType::AC);
        assert_eq!(charger.power_output_kw, 7.4);
        assert_eq!(charger.connector_types, vec![ConnectorType::Type2]);
        assert_eq!(charger.status, ChargerStatus::Available);
        assert!(charger.is_public);
        assert!(charger.is_active);
        assert!(!charger.is_deleted());
        assert_eq!(charger.version, 1);
        assert!(ChargerId::validate_id(&charger.id));
        assert!(charger.is_available());
    }

    #[test]
    fn test_create_charger_with_all_fields() {
        let station_id = StationId::generate_id();
        let pricing_info = serde_json::json!({
            "price_per_kwh": 0.25,
            "currency": "TND",
            "session_fee": 1.0
        });
        
        let charger = Charger::create(
            station_id.clone(),
            "Test Charger".to_string(),
            Some("Test Description".to_string()),
            ChargerType::DCFC,
            50.0,
            Some(400.0),
            Some(125.0),
            vec![ConnectorType::CCS, ConnectorType::CHAdeMO],
            ChargerStatus::Available,
            true,
            Some(pricing_info),
        );
        
        assert_eq!(charger.station_id, station_id);
        assert_eq!(charger.name, "Test Charger");
        assert_eq!(charger.description, Some("Test Description".to_string()));
        assert_eq!(charger.charger_type, ChargerType::DCFC);
        assert_eq!(charger.power_output_kw, 50.0);
        assert_eq!(charger.voltage, Some(400.0));
        assert_eq!(charger.current, Some(125.0));
        assert_eq!(charger.connector_types, vec![ConnectorType::CCS, ConnectorType::CHAdeMO]);
        assert_eq!(charger.status, ChargerStatus::Available);
        assert!(charger.is_public);
        assert_eq!(charger.pricing_info, Some(pricing_info));
    }

    #[test]
    fn test_update_charger() {
        let station_id = StationId::generate_id();
        let mut charger = Charger::new(
            station_id.clone(),
            "Test Charger".to_string(),
            ChargerType::AC,
            7.4,
            vec![ConnectorType::Type2],
        );
        let original_version = charger.version;
        
        charger.update(
            Some("Updated Charger".to_string()),
            Some("Updated Description".to_string()),
            Some(ChargerType::DCFC),
            Some(50.0),
            Some(400.0),
            Some(125.0),
            Some(vec![ConnectorType::CCS]),
            Some(ChargerStatus::Maintenance),
            Some(false),
            Some(serde_json::json!({"price_per_kwh": 0.30})),
            Some(false),
        );
        
        assert_eq!(charger.name, "Updated Charger");
        assert_eq!(charger.description, Some("Updated Description".to_string()));
        assert_eq!(charger.charger_type, ChargerType::DCFC);
        assert_eq!(charger.power_output_kw, 50.0);
        assert_eq!(charger.voltage, Some(400.0));
        assert_eq!(charger.current, Some(125.0));
        assert_eq!(charger.connector_types, vec![ConnectorType::CCS]);
        assert_eq!(charger.status, ChargerStatus::Maintenance);
        assert!(!charger.is_public);
        assert!(!charger.is_active);
        assert_eq!(charger.version, original_version + 1);
        assert!(!charger.is_available());
    }

    #[test]
    fn test_update_charger_status() {
        let station_id = StationId::generate_id();
        let mut charger = Charger::new(
            station_id,
            "Test Charger".to_string(),
            ChargerType::AC,
            7.4,
            vec![ConnectorType::Type2],
        );
        let original_version = charger.version;
        let original_last_update = charger.last_status_update;
        
        charger.update_status(ChargerStatus::Occupied);
        
        assert_eq!(charger.status, ChargerStatus::Occupied);
        assert_eq!(charger.version, original_version + 1);
        assert!(charger.last_status_update > original_last_update);
    }

    #[test]
    fn test_soft_delete_charger() {
        let station_id = StationId::generate_id();
        let mut charger = Charger::new(
            station_id,
            "Test Charger".to_string(),
            ChargerType::AC,
            7.4,
            vec![ConnectorType::Type2],
        );
        
        assert!(!charger.is_deleted());
        assert!(charger.is_active());
        assert!(charger.is_available());
        
        charger.delete();
        
        assert!(charger.is_deleted());
        assert!(!charger.is_active());
        assert!(!charger.is_available());
        assert!(charger.deleted_at.is_some());
    }

    #[test]
    fn test_restore_charger() {
        let station_id = StationId::generate_id();
        let mut charger = Charger::new(
            station_id,
            "Test Charger".to_string(),
            ChargerType::AC,
            7.4,
            vec![ConnectorType::Type2],
        );
        charger.delete();
        
        assert!(charger.is_deleted());
        
        charger.restore();
        
        assert!(!charger.is_deleted());
        assert!(charger.is_active());
        assert!(charger.is_available());
        assert!(charger.deleted_at.is_none());
    }

    #[test]
    fn test_validate_charger() {
        let station_id = StationId::generate_id();
        let mut charger = Charger::new(
            station_id,
            "Test Charger".to_string(),
            ChargerType::AC,
            7.4,
            vec![ConnectorType::Type2],
        );
        
        // Valid charger should pass validation
        assert!(charger.validate().is_ok());
        
        // Invalid station ID should fail
        charger.station_id = "invalid-station-id".to_string();
        assert!(charger.validate().is_err());
        
        // Valid station ID should pass
        charger.station_id = StationId::generate_id();
        assert!(charger.validate().is_ok());
        
        // Empty name should fail
        charger.name = "".to_string();
        assert!(charger.validate().is_err());
        
        // Name too long should fail
        charger.name = "a".repeat(256);
        assert!(charger.validate().is_err());
        
        // Invalid power output should fail
        charger.name = "Test Charger".to_string();
        charger.power_output_kw = 0.0;
        assert!(charger.validate().is_err());
        
        // Power output too high should fail
        charger.power_output_kw = 1001.0;
        assert!(charger.validate().is_err());
        
        // Invalid voltage should fail
        charger.power_output_kw = 7.4;
        charger.voltage = Some(0.0);
        assert!(charger.validate().is_err());
        
        // Voltage too high should fail
        charger.voltage = Some(1001.0);
        assert!(charger.validate().is_err());
        
        // Invalid current should fail
        charger.voltage = None;
        charger.current = Some(0.0);
        assert!(charger.validate().is_err());
        
        // Current too high should fail
        charger.current = Some(1001.0);
        assert!(charger.validate().is_err());
        
        // Empty connector types should fail
        charger.current = None;
        charger.connector_types = vec![];
        assert!(charger.validate().is_err());
        
        // Invalid version should fail
        charger.connector_types = vec![ConnectorType::Type2];
        charger.version = 0;
        assert!(charger.validate().is_err());
    }

    #[test]
    fn test_charger_availability() {
        let station_id = StationId::generate_id();
        let mut charger = Charger::new(
            station_id,
            "Test Charger".to_string(),
            ChargerType::AC,
            7.4,
            vec![ConnectorType::Type2],
        );
        
        // Available charger
        assert!(charger.is_available());
        
        // Update status to occupied
        charger.update_status(ChargerStatus::Occupied);
        assert!(!charger.is_available());
        
        // Update status to offline
        charger.update_status(ChargerStatus::Offline);
        assert!(!charger.is_available());
        
        // Update status to maintenance
        charger.update_status(ChargerStatus::Maintenance);
        assert!(!charger.is_available());
        
        // Update status to reserved
        charger.update_status(ChargerStatus::Reserved);
        assert!(!charger.is_available());
        
        // Update status to faulted
        charger.update_status(ChargerStatus::Faulted);
        assert!(!charger.is_available());
        
        // Soft delete charger
        charger.delete();
        assert!(!charger.is_available());
        
        // Restore charger
        charger.restore();
        charger.update_status(ChargerStatus::Available);
        assert!(charger.is_available());
    }
}