use crate::models::{Charger, ChargerType, ChargerStatus, ConnectorType};
use crate::repositories::ChargerRepository;
use crate::utils::database::Database;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChargerServiceError {
    #[error("Charger not found: {0}")]
    NotFound(String),
    #[error("Validation error: {0}")]
    Validation(String),
    #[error("Optimistic lock error: {0}")]
    OptimisticLock(String),
    #[error("Database error: {0}")]
    Database(String),
    #[error("Station not found: {0}")]
    StationNotFound(String),
    #[error("Station is soft-deleted: {0}")]
    StationSoftDeleted(String),
    #[error("Charger already exists with name: {0}")]
    NameAlreadyExists(String),
    #[error("Charger is soft-deleted: {0}")]
    SoftDeleted(String),
    #[error("Invalid status transition: {0} -> {1}")]
    InvalidStatusTransition(String, String),
}

impl From<sqlx::Error> for ChargerServiceError {
    fn from(err: sqlx::Error) -> Self {
        ChargerServiceError::Database(err.to_string())
    }
}

impl From<crate::services::StationServiceError> for ChargerServiceError {
    fn from(err: crate::services::StationServiceError) -> Self {
        match err {
            crate::services::StationServiceError::NotFound(msg) => 
                ChargerServiceError::StationNotFound(msg),
            crate::services::StationServiceError::SoftDeleted(msg) => 
                ChargerServiceError::StationSoftDeleted(msg),
            crate::services::StationServiceError::Validation(msg) => 
                ChargerServiceError::Validation(msg),
            crate::services::StationServiceError::Database(msg) => 
                ChargerServiceError::Database(msg),
            _ => ChargerServiceError::Database(err.to_string()),
        }
    }
}

pub struct ChargerService {
    repository: ChargerRepository,
    station_service: Arc<crate::services::StationService>,
}

impl ChargerService {
    /// Create a new ChargerService
    pub fn new(db: Arc<Database>, station_service: Arc<crate::services::StationService>) -> Self {
        Self {
            repository: ChargerRepository::new(db),
            station_service,
        }
    }

    /// Validate status transition
    fn validate_status_transition(from: &ChargerStatus, to: &ChargerStatus) -> Result<(), ChargerServiceError> {
        match (from, to) {
            // Allow any transition from Available
            (ChargerStatus::Available, _) => Ok(()),
            
            // Allow transition from Occupied to Available, Offline, Maintenance, or Faulted
            (ChargerStatus::Occupied, ChargerStatus::Available) => Ok(()),
            (ChargerStatus::Occupied, ChargerStatus::Offline) => Ok(()),
            (ChargerStatus::Occupied, ChargerStatus::Maintenance) => Ok(()),
            (ChargerStatus::Occupied, ChargerStatus::Faulted) => Ok(()),
            
            // Allow transition from Offline to Available, Maintenance, or Faulted
            (ChargerStatus::Offline, ChargerStatus::Available) => Ok(()),
            (ChargerStatus::Offline, ChargerStatus::Maintenance) => Ok(()),
            (ChargerStatus::Offline, ChargerStatus::Faulted) => Ok(()),
            
            // Allow transition from Maintenance to Available or Offline
            (ChargerStatus::Maintenance, ChargerStatus::Available) => Ok(()),
            (ChargerStatus::Maintenance, ChargerStatus::Offline) => Ok(()),
            
            // Allow transition from Reserved to Available or Occupied
            (ChargerStatus::Reserved, ChargerStatus::Available) => Ok(()),
            (ChargerStatus::Reserved, ChargerStatus::Occupied) => Ok(()),
            
            // Allow transition from Faulted to Maintenance or Offline
            (ChargerStatus::Faulted, ChargerStatus::Maintenance) => Ok(()),
            (ChargerStatus::Faulted, ChargerStatus::Offline) => Ok(()),
            
            // Invalid transitions
            (from, to) => Err(ChargerServiceError::InvalidStatusTransition(
                format!("{:?}", from),
                format!("{:?}", to)
            )),
        }
    }

    /// Create a new charger
    pub async fn create_charger(
        &self,
        station_id: String,
        name: String,
        description: Option<String>,
        charger_type: ChargerType,
        power_output_kw: f64,
        voltage: Option<f64>,
        current: Option<f64>,
        connector_types: Vec<ConnectorType>,
        is_public: Option<bool>,
        pricing_info: Option<serde_json::Value>,
    ) -> Result<Charger, ChargerServiceError> {
        // Validate input data
        if name.trim().is_empty() {
            return Err(ChargerServiceError::Validation("Charger name cannot be empty".to_string()));
        }

        if name.len() > 255 {
            return Err(ChargerServiceError::Validation("Charger name cannot exceed 255 characters".to_string()));
        }

        if power_output_kw <= 0.0 {
            return Err(ChargerServiceError::Validation("Power output must be greater than 0".to_string()));
        }

        if power_output_kw > 1000.0 {
            return Err(ChargerServiceError::Validation("Power output cannot exceed 1000 kW".to_string()));
        }

        if let Some(voltage) = voltage {
            if voltage <= 0.0 {
                return Err(ChargerServiceError::Validation("Voltage must be greater than 0".to_string()));
            }
            if voltage > 1000.0 {
                return Err(ChargerServiceError::Validation("Voltage cannot exceed 1000V".to_string()));
            }
        }

        if let Some(current) = current {
            if current <= 0.0 {
                return Err(ChargerServiceError::Validation("Current must be greater than 0".to_string()));
            }
            if current > 1000.0 {
                return Err(ChargerServiceError::Validation("Current cannot exceed 1000A".to_string()));
            }
        }

        if connector_types.is_empty() {
            return Err(ChargerServiceError::Validation("At least one connector type is required".to_string()));
        }

        // Validate station exists and is active
        match self.station_service.get_station(&station_id).await {
            Ok(_) => {},
            Err(crate::services::StationServiceError::NotFound(_)) => {
                return Err(ChargerServiceError::StationNotFound(station_id));
            },
            Err(e) => return Err(ChargerServiceError::Database(e.to_string())),
        }

        // Create charger
        let charger = Charger::create(
            station_id,
            name,
            description,
            charger_type,
            power_output_kw,
            voltage,
            current,
            connector_types,
            ChargerStatus::Available,
            is_public.unwrap_or(true),
            pricing_info,
        );

        // Validate charger data
        if let Err(err) = charger.validate() {
            return Err(ChargerServiceError::Validation(err));
        }

        // Save to database
        let saved_charger = self.repository.create(&charger).await?;
        Ok(saved_charger)
    }

    /// Get a charger by ID
    pub async fn get_charger(&self, id: &str) -> Result<Charger, ChargerServiceError> {
        // Validate charger ID format
        if !crate::models::ChargerId::validate_id(id) {
            return Err(ChargerServiceError::Validation("Invalid charger ID format".to_string()));
        }

        let charger = self.repository.find_by_id(id).await?
            .ok_or_else(|| ChargerServiceError::NotFound(id.to_string()))?;

        Ok(charger)
    }

    /// Get a charger by ID (including soft-deleted records)
    pub async fn get_charger_including_deleted(&self, id: &str) -> Result<Charger, ChargerServiceError> {
        // Validate charger ID format
        if !crate::models::ChargerId::validate_id(id) {
            return Err(ChargerServiceError::Validation("Invalid charger ID format".to_string()));
        }

        let charger = self.repository.find_by_id_including_deleted(id).await?
            .ok_or_else(|| ChargerServiceError::NotFound(id.to_string()))?;

        Ok(charger)
    }

    /// Get all chargers for a station
    pub async fn get_chargers_by_station(&self, station_id: &str) -> Result<Vec<Charger>, ChargerServiceError> {
        // Validate station exists and is active
        match self.station_service.get_station(station_id).await {
            Ok(_) => {},
            Err(crate::services::StationServiceError::NotFound(_)) => {
                return Err(ChargerServiceError::StationNotFound(station_id.to_string()));
            },
            Err(e) => return Err(ChargerServiceError::Database(e.to_string())),
        }

        let chargers = self.repository.find_by_station(station_id).await?;
        Ok(chargers)
    }

    /// Get all active chargers
    pub async fn get_all_chargers(&self) -> Result<Vec<Charger>, ChargerServiceError> {
        let chargers = self.repository.find_all().await?;
        Ok(chargers)
    }

    /// Update a charger
    pub async fn update_charger(
        &self,
        id: &str,
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
    ) -> Result<Charger, ChargerServiceError> {
        // Validate charger ID format
        if !crate::models::ChargerId::validate_id(id) {
            return Err(ChargerServiceError::Validation("Invalid charger ID format".to_string()));
        }

        // Get current charger
        let mut charger = self.get_charger(id).await?;

        // Validate station if changing
        match self.station_service.get_station(&charger.station_id).await {
            Ok(_) => {},
            Err(crate::services::StationServiceError::NotFound(_)) => {
                return Err(ChargerServiceError::StationNotFound(charger.station_id.clone()));
            },
            Err(e) => return Err(ChargerServiceError::Database(e.to_string())),
        }

        // Validate status transition
        if let Some(new_status) = status {
            if new_status != charger.status {
                self.validate_status_transition(&charger.status, &new_status)?;
            }
        }

        // Update charger data
        charger.update(
            name,
            description,
            charger_type,
            power_output_kw,
            voltage,
            current,
            connector_types,
            status,
            is_public,
            pricing_info,
            is_active,
        );

        // Validate updated charger
        if let Err(err) = charger.validate() {
            return Err(ChargerServiceError::Validation(err));
        }

        // Save to database
        match self.repository.update(&charger).await {
            Ok(updated_charger) => Ok(updated_charger),
            Err(_) => Err(ChargerServiceError::OptimisticLock(
                "Charger was modified by another transaction".to_string()
            )),
        }
    }

    /// Update charger status only
    pub async fn update_charger_status(&self, id: &str, status: ChargerStatus) -> Result<bool, ChargerServiceError> {
        // Validate charger ID format
        if !crate::models::ChargerId::validate_id(id) {
            return Err(ChargerServiceError::Validation("Invalid charger ID format".to_string()));
        }

        // Get current charger
        let charger = self.get_charger(id).await?;

        // Validate status transition
        if status != charger.status {
            self.validate_status_transition(&charger.status, &status)?;
        }

        // Update status
        match self.repository.update_status(id, status).await {
            Ok(success) => Ok(success),
            Err(_) => Err(ChargerServiceError::OptimisticLock(
                "Charger was modified by another transaction".to_string()
            )),
        }
    }

    /// Soft delete a charger
    pub async fn delete_charger(&self, id: &str) -> Result<bool, ChargerServiceError> {
        // Validate charger ID format
        if !crate::models::ChargerId::validate_id(id) {
            return Err(ChargerServiceError::Validation("Invalid charger ID format".to_string()));
        }

        // Get current charger to check version
        let charger = self.get_charger(id).await?;

        // Delete charger
        match self.repository.delete(id, charger.version).await {
            Ok(success) => {
                if success {
                    Ok(true)
                } else {
                    Err(ChargerServiceError::OptimisticLock(
                        "Charger was modified by another transaction".to_string()
                    ))
                }
            }
            Err(_) => Err(ChargerServiceError::OptimisticLock(
                "Charger was modified by another transaction".to_string()
            )),
        }
    }

    /// Restore a soft-deleted charger
    pub async fn restore_charger(&self, id: &str) -> Result<bool, ChargerServiceError> {
        // Validate charger ID format
        if !crate::models::ChargerId::validate_id(id) {
            return Err(ChargerServiceError::Validation("Invalid charger ID format".to_string()));
        }

        // Get current charger (including deleted) to check version
        let charger = self.get_charger_including_deleted(id).await?;

        if !charger.is_deleted() {
            return Err(ChargerServiceError::Validation("Charger is not deleted".to_string()));
        }

        // Check if station is active
        match self.station_service.get_station(&charger.station_id).await {
            Ok(_) => {},
            Err(crate::services::StationServiceError::NotFound(_)) => {
                return Err(ChargerServiceError::StationNotFound(charger.station_id.clone()));
            },
            Err(e) => return Err(ChargerServiceError::Database(e.to_string())),
        }

        // Restore charger
        match self.repository.restore(id, charger.version).await {
            Ok(success) => {
                if success {
                    Ok(true)
                } else {
                    Err(ChargerServiceError::OptimisticLock(
                        "Charger was modified by another transaction".to_string()
                    ))
                }
            }
            Err(_) => Err(ChargerServiceError::OptimisticLock(
                "Charger was modified by another transaction".to_string()
            )),
        }
    }

    /// Search chargers by name
    pub async fn search_chargers_by_name(&self, name: &str) -> Result<Vec<Charger>, ChargerServiceError> {
        if name.trim().is_empty() {
            return Err(ChargerServiceError::Validation("Search term cannot be empty".to_string()));
        }

        if name.len() > 255 {
            return Err(ChargerServiceError::Validation("Search term cannot exceed 255 characters".to_string()));
        }

        let chargers = self.repository.find_by_name(name).await?;
        Ok(chargers)
    }

    /// Find chargers by status
    pub async fn find_chargers_by_status(&self, status: ChargerStatus) -> Result<Vec<Charger>, ChargerServiceError> {
        let chargers = self.repository.find_by_status(status).await?;
        Ok(chargers)
    }

    /// Find available chargers
    pub async fn find_available_chargers(&self) -> Result<Vec<Charger>, ChargerServiceError> {
        let chargers = self.repository.find_available().await?;
        Ok(chargers)
    }

    /// Find chargers by charger type
    pub async fn find_chargers_by_type(&self, charger_type: ChargerType) -> Result<Vec<Charger>, ChargerServiceError> {
        let chargers = self.repository.find_by_charger_type(charger_type).await?;
        Ok(chargers)
    }

    /// Find chargers by connector type
    pub async fn find_chargers_by_connector_type(&self, connector_type: ConnectorType) -> Result<Vec<Charger>, ChargerServiceError> {
        let chargers = self.repository.find_by_connector_type(connector_type).await?;
        Ok(chargers)
    }

    /// Find public chargers
    pub async fn find_public_chargers(&self) -> Result<Vec<Charger>, ChargerServiceError> {
        let chargers = self.repository.find_public().await?;
        Ok(chargers)
    }

    /// Find chargers created within a date range
    pub async fn find_chargers_created_between(
        &self,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<Charger>, ChargerServiceError> {
        if start > end {
            return Err(ChargerServiceError::Validation("Start date must be before end date".to_string()));
        }

        let chargers = self.repository.find_by_created_range(start, end).await?;
        Ok(chargers)
    }

    /// Find chargers updated within a date range
    pub async fn find_chargers_updated_between(
        &self,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<Charger>, ChargerServiceError> {
        if start > end {
            return Err(ChargerServiceError::Validation("Start date must be before end date".to_string()));
        }

        let chargers = self.repository.find_by_updated_range(start, end).await?;
        Ok(chargers)
    }

    /// Check if a charger exists
    pub async fn charger_exists(&self, id: &str) -> Result<bool, ChargerServiceError> {
        // Validate charger ID format
        if !crate::models::ChargerId::validate_id(id) {
            return Err(ChargerServiceError::Validation("Invalid charger ID format".to_string()));
        }

        let exists = self.repository.exists(id).await?;
        Ok(exists)
    }

    /// Get charger count for a station
    pub async fn get_charger_count_by_station(&self, station_id: &str) -> Result<i64, ChargerServiceError> {
        // Validate station exists and is active
        match self.station_service.get_station(station_id).await {
            Ok(_) => {},
            Err(crate::services::StationServiceError::NotFound(_)) => {
                return Err(ChargerServiceError::StationNotFound(station_id.to_string()));
            },
            Err(e) => return Err(ChargerServiceError::Database(e.to_string())),
        }

        let count = self.repository.count_by_station(station_id).await?;
        Ok(count)
    }

    /// Get total charger count
    pub async fn get_charger_count(&self) -> Result<i64, ChargerServiceError> {
        let count = self.repository.count().await?;
        Ok(count)
    }

    /// Get available charger count
    pub async fn get_available_charger_count(&self) -> Result<i64, ChargerServiceError> {
        let count = self.repository.count_available().await?;
        Ok(count)
    }

    /// Get charger version
    pub async fn get_charger_version(&self, id: &str) -> Result<Option<i32>, ChargerServiceError> {
        // Validate charger ID format
        if !crate::models::ChargerId::validate_id(id) {
            return Err(ChargerServiceError::Validation("Invalid charger ID format".to_string()));
        }

        let version = self.repository.get_version(id).await?;
        Ok(version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ChargerId, StationId, ChargerType, ChargerStatus, ConnectorType};
    
    #[tokio::test]
    async fn test_create_charger_validation() {
        // This test would require a test database
        let db = Arc::new(Database::new("postgresql://test:test@localhost/test").await.unwrap());
        let station_service = Arc::new(crate::services::StationService::new(
            db.clone(), 
            Arc::new(crate::services::CompanyService::new(db.clone()))
        ));
        let service = ChargerService::new(db, station_service);
        
        let station_id = StationId::generate_id();
        
        // Test empty name
        let result = service.create_charger(
            station_id.clone(),
            "".to_string(),
            None,
            ChargerType::AC,
            7.4,
            None,
            None,
            vec![ConnectorType::Type2],
            None,
            None,
        ).await;
        assert!(matches!(result, Err(ChargerServiceError::Validation(_))));
        
        // Test name too long
        let result = service.create_charger(
            station_id.clone(),
            "a".repeat(256),
            None,
            ChargerType::AC,
            7.4,
            None,
            None,
            vec![ConnectorType::Type2],
            None,
            None,
        ).await;
        assert!(matches!(result, Err(ChargerServiceError::Validation(_))));
        
        // Test invalid power output
        let result = service.create_charger(
            station_id.clone(),
            "Test Charger".to_string(),
            None,
            ChargerType::AC,
            0.0,
            None,
            None,
            vec![ConnectorType::Type2],
            None,
            None,
        ).await;
        assert!(matches!(result, Err(ChargerServiceError::Validation(_))));
        
        // Test empty connector types
        let result = service.create_charger(
            station_id.clone(),
            "Test Charger".to_string(),
            None,
            ChargerType::AC,
            7.4,
            None,
            None,
            vec![],
            None,
            None,
        ).await;
        assert!(matches!(result, Err(ChargerServiceError::Validation(_))));
        
        // Test invalid charger ID
        let result = service.get_charger("invalid-id").await;
        assert!(matches!(result, Err(ChargerServiceError::Validation(_))));
    }
    
    #[tokio::test]
    async fn test_status_transitions() {
        let db = Arc::new(Database::new("postgresql://test:test@localhost/test").await.unwrap());
        let station_service = Arc::new(crate::services::StationService::new(
            db.clone(), 
            Arc::new(crate::services::CompanyService::new(db.clone()))
        ));
        let service = ChargerService::new(db, station_service);
        
        // Test valid transitions
        assert!(service.validate_status_transition(&ChargerStatus::Available, &ChargerStatus::Occupied).is_ok());
        assert!(service.validate_status_transition(&ChargerStatus::Occupied, &ChargerStatus::Available).is_ok());
        assert!(service.validate_status_transition(&ChargerStatus::Offline, &ChargerStatus::Maintenance).is_ok());
        
        // Test invalid transitions
        assert!(service.validate_status_transition(&ChargerStatus::Occupied, &ChargerStatus::Reserved).is_err());
        assert!(service.validate_status_transition(&ChargerStatus::Maintenance, &ChargerStatus::Occupied).is_err());
    }
}