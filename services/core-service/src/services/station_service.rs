use crate::models::{Station, AccessType};
use crate::repositories::StationRepository;
use crate::utils::database::Database;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StationServiceError {
    #[error("Station not found: {0}")]
    NotFound(String),
    #[error("Validation error: {0}")]
    Validation(String),
    #[error("Optimistic lock error: {0}")]
    OptimisticLock(String),
    #[error("Database error: {0}")]
    Database(String),
    #[error("Company not found: {0}")]
    CompanyNotFound(String),
    #[error("Company is soft-deleted: {0}")]
    CompanySoftDeleted(String),
    #[error("Station already exists with name: {0}")]
    NameAlreadyExists(String),
    #[error("Station is soft-deleted: {0}")]
    SoftDeleted(String),
}

impl From<sqlx::Error> for StationServiceError {
    fn from(err: sqlx::Error) -> Self {
        StationServiceError::Database(err.to_string())
    }
}

pub struct StationService {
    repository: StationRepository,
    company_service: Arc<crate::services::CompanyService>,
}

impl StationService {
    /// Create a new StationService
    pub fn new(db: Arc<Database>, company_service: Arc<crate::services::CompanyService>) -> Self {
        Self {
            repository: StationRepository::new(db),
            company_service,
        }
    }

    /// Create a new station
    pub async fn create_station(
        &self,
        company_id: String,
        name: String,
        description: Option<String>,
        address: String,
        latitude: f64,
        longitude: f64,
        phone: Option<String>,
        email: Option<String>,
        website: Option<String>,
        access_type: Option<AccessType>,
        operating_hours: Option<serde_json::Value>,
        amenities: Option<Vec<String>>,
    ) -> Result<Station, StationServiceError> {
        // Validate input data
        if name.trim().is_empty() {
            return Err(StationServiceError::Validation("Station name cannot be empty".to_string()));
        }

        if name.len() > 255 {
            return Err(StationServiceError::Validation("Station name cannot exceed 255 characters".to_string()));
        }

        if address.trim().is_empty() {
            return Err(StationServiceError::Validation("Station address cannot be empty".to_string()));
        }

        if latitude < -90.0 || latitude > 90.0 {
            return Err(StationServiceError::Validation("Latitude must be between -90 and 90".to_string()));
        }

        if longitude < -180.0 || longitude > 180.0 {
            return Err(StationServiceError::Validation("Longitude must be between -180 and 180".to_string()));
        }

        // Validate company exists and is active
        match self.company_service.get_company(&company_id).await {
            Ok(_) => {},
            Err(crate::services::CompanyServiceError::NotFound(_)) => {
                return Err(StationServiceError::CompanyNotFound(company_id));
            },
            Err(e) => return Err(StationServiceError::Database(e.to_string())),
        }

        // Create station
        let station = Station::create(
            company_id,
            name,
            description,
            address,
            latitude,
            longitude,
            phone,
            email,
            website,
            access_type.unwrap_or(AccessType::Public),
            operating_hours,
            amenities,
        );

        // Validate station data
        if let Err(err) = station.validate() {
            return Err(StationServiceError::Validation(err));
        }

        // Save to database
        let saved_station = self.repository.create(&station).await?;
        Ok(saved_station)
    }

    /// Get a station by ID
    pub async fn get_station(&self, id: &str) -> Result<Station, StationServiceError> {
        // Validate station ID format
        if !crate::models::StationId::validate_id(id) {
            return Err(StationServiceError::Validation("Invalid station ID format".to_string()));
        }

        let station = self.repository.find_by_id(id).await?
            .ok_or_else(|| StationServiceError::NotFound(id.to_string()))?;

        Ok(station)
    }

    /// Get a station by ID (including soft-deleted records)
    pub async fn get_station_including_deleted(&self, id: &str) -> Result<Station, StationServiceError> {
        // Validate station ID format
        if !crate::models::StationId::validate_id(id) {
            return Err(StationServiceError::Validation("Invalid station ID format".to_string()));
        }

        let station = self.repository.find_by_id_including_deleted(id).await?
            .ok_or_else(|| StationServiceError::NotFound(id.to_string()))?;

        Ok(station)
    }

    /// Get all stations for a company
    pub async fn get_stations_by_company(&self, company_id: &str) -> Result<Vec<Station>, StationServiceError> {
        // Validate company exists and is active
        match self.company_service.get_company(company_id).await {
            Ok(_) => {},
            Err(crate::services::CompanyServiceError::NotFound(_)) => {
                return Err(StationServiceError::CompanyNotFound(company_id.to_string()));
            },
            Err(e) => return Err(StationServiceError::Database(e.to_string())),
        }

        let stations = self.repository.find_by_company(company_id).await?;
        Ok(stations)
    }

    /// Get all active stations
    pub async fn get_all_stations(&self) -> Result<Vec<Station>, StationServiceError> {
        let stations = self.repository.find_all().await?;
        Ok(stations)
    }

    /// Update a station
    pub async fn update_station(
        &self,
        id: &str,
        name: Option<String>,
        description: Option<String>,
        address: Option<String>,
        latitude: Option<f64>,
        longitude: Option<f64>,
        phone: Option<String>,
        email: Option<String>,
        website: Option<String>,
        access_type: Option<AccessType>,
        operating_hours: Option<serde_json::Value>,
        amenities: Option<Vec<String>>,
        is_active: Option<bool>,
    ) -> Result<Station, StationServiceError> {
        // Validate station ID format
        if !crate::models::StationId::validate_id(id) {
            return Err(StationServiceError::Validation("Invalid station ID format".to_string()));
        }

        // Get current station
        let mut station = self.get_station(id).await?;

        // Validate company if changing
        if let Some(ref new_company_id) = name {
            // This is a bit of a hack - we're using name field to check if company_id is being changed
            // In a real implementation, we would have a separate field for company_id
            match self.company_service.get_company(&station.company_id).await {
                Ok(_) => {},
                Err(crate::services::CompanyServiceError::NotFound(_)) => {
                    return Err(StationServiceError::CompanyNotFound(station.company_id.clone()));
                },
                Err(e) => return Err(StationServiceError::Database(e.to_string())),
            }
        }

        // Update station data
        station.update(
            name,
            description,
            address,
            latitude,
            longitude,
            phone,
            email,
            website,
            access_type,
            operating_hours,
            amenities,
            is_active,
        );

        // Validate updated station
        if let Err(err) = station.validate() {
            return Err(StationServiceError::Validation(err));
        }

        // Save to database
        match self.repository.update(&station).await {
            Ok(updated_station) => Ok(updated_station),
            Err(_) => Err(StationServiceError::OptimisticLock(
                "Station was modified by another transaction".to_string()
            )),
        }
    }

    /// Soft delete a station
    pub async fn delete_station(&self, id: &str) -> Result<bool, StationServiceError> {
        // Validate station ID format
        if !crate::models::StationId::validate_id(id) {
            return Err(StationServiceError::Validation("Invalid station ID format".to_string()));
        }

        // Get current station to check version
        let station = self.get_station(id).await?;

        // Delete station
        match self.repository.delete(id, station.version).await {
            Ok(success) => {
                if success {
                    Ok(true)
                } else {
                    Err(StationServiceError::OptimisticLock(
                        "Station was modified by another transaction".to_string()
                    ))
                }
            }
            Err(_) => Err(StationServiceError::OptimisticLock(
                "Station was modified by another transaction".to_string()
            )),
        }
    }

    /// Restore a soft-deleted station
    pub async fn restore_station(&self, id: &str) -> Result<bool, StationServiceError> {
        // Validate station ID format
        if !crate::models::StationId::validate_id(id) {
            return Err(StationServiceError::Validation("Invalid station ID format".to_string()));
        }

        // Get current station (including deleted) to check version
        let station = self.get_station_including_deleted(id).await?;

        if !station.is_deleted() {
            return Err(StationServiceError::Validation("Station is not deleted".to_string()));
        }

        // Check if company is active
        match self.company_service.get_company(&station.company_id).await {
            Ok(_) => {},
            Err(crate::services::CompanyServiceError::NotFound(_)) => {
                return Err(StationServiceError::CompanyNotFound(station.company_id.clone()));
            },
            Err(e) => return Err(StationServiceError::Database(e.to_string())),
        }

        // Restore station
        match self.repository.restore(id, station.version).await {
            Ok(success) => {
                if success {
                    Ok(true)
                } else {
                    Err(StationServiceError::OptimisticLock(
                        "Station was modified by another transaction".to_string()
                    ))
                }
            }
            Err(_) => Err(StationServiceError::OptimisticLock(
                "Station was modified by another transaction".to_string()
            )),
        }
    }

    /// Search stations by name
    pub async fn search_stations_by_name(&self, name: &str) -> Result<Vec<Station>, StationServiceError> {
        if name.trim().is_empty() {
            return Err(StationServiceError::Validation("Search term cannot be empty".to_string()));
        }

        if name.len() > 255 {
            return Err(StationServiceError::Validation("Search term cannot exceed 255 characters".to_string()));
        }

        let stations = self.repository.find_by_name(name).await?;
        Ok(stations)
    }

    /// Find stations within a geographic radius
    pub async fn find_stations_by_radius(
        &self,
        center_lat: f64,
        center_lon: f64,
        radius_km: f64,
    ) -> Result<Vec<Station>, StationServiceError> {
        if center_lat < -90.0 || center_lat > 90.0 {
            return Err(StationServiceError::Validation("Center latitude must be between -90 and 90".to_string()));
        }

        if center_lon < -180.0 || center_lon > 180.0 {
            return Err(StationServiceError::Validation("Center longitude must be between -180 and 180".to_string()));
        }

        if radius_km <= 0.0 || radius_km > 1000.0 {
            return Err(StationServiceError::Validation("Radius must be between 0 and 1000 km".to_string()));
        }

        let stations = self.repository.find_by_radius(center_lat, center_lon, radius_km).await?;
        Ok(stations)
    }

    /// Find stations by access type
    pub async fn find_stations_by_access_type(&self, access_type: AccessType) -> Result<Vec<Station>, StationServiceError> {
        let stations = self.repository.find_by_access_type(access_type).await?;
        Ok(stations)
    }

    /// Find stations created within a date range
    pub async fn find_stations_created_between(
        &self,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<Station>, StationServiceError> {
        if start > end {
            return Err(StationServiceError::Validation("Start date must be before end date".to_string()));
        }

        let stations = self.repository.find_by_created_range(start, end).await?;
        Ok(stations)
    }

    /// Find stations updated within a date range
    pub async fn find_stations_updated_between(
        &self,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<Station>, StationServiceError> {
        if start > end {
            return Err(StationServiceError::Validation("Start date must be before end date".to_string()));
        }

        let stations = self.repository.find_by_updated_range(start, end).await?;
        Ok(stations)
    }

    /// Check if a station exists
    pub async fn station_exists(&self, id: &str) -> Result<bool, StationServiceError> {
        // Validate station ID format
        if !crate::models::StationId::validate_id(id) {
            return Err(StationServiceError::Validation("Invalid station ID format".to_string()));
        }

        let exists = self.repository.exists(id).await?;
        Ok(exists)
    }

    /// Get station count for a company
    pub async fn get_station_count_by_company(&self, company_id: &str) -> Result<i64, StationServiceError> {
        // Validate company exists and is active
        match self.company_service.get_company(company_id).await {
            Ok(_) => {},
            Err(crate::services::CompanyServiceError::NotFound(_)) => {
                return Err(StationServiceError::CompanyNotFound(company_id.to_string()));
            },
            Err(e) => return Err(StationServiceError::Database(e.to_string())),
        }

        let count = self.repository.count_by_company(company_id).await?;
        Ok(count)
    }

    /// Get total station count
    pub async fn get_station_count(&self) -> Result<i64, StationServiceError> {
        let count = self.repository.count().await?;
        Ok(count)
    }

    /// Get station version
    pub async fn get_station_version(&self, id: &str) -> Result<Option<i32>, StationServiceError> {
        // Validate station ID format
        if !crate::models::StationId::validate_id(id) {
            return Err(StationServiceError::Validation("Invalid station ID format".to_string()));
        }

        let version = self.repository.get_version(id).await?;
        Ok(version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{StationId, CompanyId, AccessType};
    
    #[tokio::test]
    async fn test_create_station_validation() {
        // This test would require a test database
        let db = Arc::new(Database::new("postgresql://test:test@localhost/test").await.unwrap());
        let company_service = Arc::new(crate::services::CompanyService::new(db.clone()));
        let service = StationService::new(db, company_service);
        
        let company_id = CompanyId::generate_id();
        
        // Test empty name
        let result = service.create_station(
            company_id.clone(),
            "".to_string(),
            None,
            "Test Address".to_string(),
            36.8065,
            10.1815,
            None,
            None,
            None,
            None,
            None,
            None,
        ).await;
        assert!(matches!(result, Err(StationServiceError::Validation(_))));
        
        // Test name too long
        let result = service.create_station(
            company_id.clone(),
            "a".repeat(256),
            None,
            "Test Address".to_string(),
            36.8065,
            10.1815,
            None,
            None,
            None,
            None,
            None,
            None,
        ).await;
        assert!(matches!(result, Err(StationServiceError::Validation(_))));
        
        // Test empty address
        let result = service.create_station(
            company_id.clone(),
            "Test Station".to_string(),
            None,
            "".to_string(),
            36.8065,
            10.1815,
            None,
            None,
            None,
            None,
            None,
            None,
        ).await;
        assert!(matches!(result, Err(StationServiceError::Validation(_))));
        
        // Test invalid latitude
        let result = service.create_station(
            company_id.clone(),
            "Test Station".to_string(),
            None,
            "Test Address".to_string(),
            91.0,
            10.1815,
            None,
            None,
            None,
            None,
            None,
            None,
        ).await;
        assert!(matches!(result, Err(StationServiceError::Validation(_))));
        
        // Test invalid longitude
        let result = service.create_station(
            company_id,
            "Test Station".to_string(),
            None,
            "Test Address".to_string(),
            36.8065,
            181.0,
            None,
            None,
            None,
            None,
            None,
            None,
        ).await;
        assert!(matches!(result, Err(StationServiceError::Validation(_))));
        
        // Test invalid station ID
        let result = service.get_station("invalid-id").await;
        assert!(matches!(result, Err(StationServiceError::Validation(_))));
    }
}