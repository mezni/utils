use crate::models::{BaseModel, FullModel, StationId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Type};
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, Validate)]
pub struct Station {
    #[serde(flatten)]
    pub base: BaseModel,
    pub company_id: String,
    pub name: String,
    pub description: Option<String>,
    pub address: String,
    pub latitude: f64,
    pub longitude: f64,
    pub phone: Option<String>,
    pub email: Option<String>,
    pub website: Option<String>,
    pub access_type: AccessType,
    pub operating_hours: Option<serde_json::Value>,
    pub amenities: Option<Vec<String>>,
    pub is_active: bool,
    #[serde(skip)]
    pub deleted_at: Option<DateTime<Utc>>,
    pub version: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type, sqlx::Type)]
#[sqlx(type_name = "access_type")]
pub enum AccessType {
    Public,
    Private,
    Restricted,
}

impl Station {
    /// Create a new station with default values
    pub fn new(company_id: String, name: String, address: String, latitude: f64, longitude: f64) -> Self {
        let now = Utc::now();
        Self {
            base: BaseModel {
                id: StationId::generate_id(),
                created_at: now,
                updated_at: now,
            },
            company_id,
            name,
            description: None,
            address,
            latitude,
            longitude,
            phone: None,
            email: None,
            website: None,
            access_type: AccessType::Public,
            operating_hours: None,
            amenities: None,
            is_active: true,
            deleted_at: None,
            version: 1,
        }
    }

    /// Create a station with all fields
    pub fn create(
        company_id: String,
        name: String,
        description: Option<String>,
        address: String,
        latitude: f64,
        longitude: f64,
        phone: Option<String>,
        email: Option<String>,
        website: Option<String>,
        access_type: AccessType,
        operating_hours: Option<serde_json::Value>,
        amenities: Option<Vec<String>>,
    ) -> Self {
        let now = Utc::now();
        Self {
            base: BaseModel {
                id: StationId::generate_id(),
                created_at: now,
                updated_at: now,
            },
            company_id,
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
            is_active: true,
            deleted_at: None,
            version: 1,
        }
    }

    /// Update station information
    pub fn update(
        &mut self,
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
    ) {
        let now = Utc::now();
        
        if let Some(name) = name {
            self.name = name;
        }
        if let Some(description) = description {
            self.description = description;
        }
        if let Some(address) = address {
            self.address = address;
        }
        if let Some(latitude) = latitude {
            self.latitude = latitude;
        }
        if let Some(longitude) = longitude {
            self.longitude = longitude;
        }
        if let Some(phone) = phone {
            self.phone = phone;
        }
        if let Some(email) = email {
            self.email = email;
        }
        if let Some(website) = website {
            self.website = website;
        }
        if let Some(access_type) = access_type {
            self.access_type = access_type;
        }
        if let Some(operating_hours) = operating_hours {
            self.operating_hours = operating_hours;
        }
        if let Some(amenities) = amenities {
            self.amenities = amenities;
        }
        if let Some(is_active) = is_active {
            self.is_active = is_active;
        }
        
        self.base.updated_at = now;
        self.version += 1;
    }

    /// Soft delete the station
    pub fn delete(&mut self) {
        self.deleted_at = Some(Utc::now());
        self.base.updated_at = Utc::now();
        self.version += 1;
    }

    /// Restore the station (undo soft delete)
    pub fn restore(&mut self) {
        self.deleted_at = None;
        self.base.updated_at = Utc::now();
        self.version += 1;
    }

    /// Check if the station is deleted
    pub fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }

    /// Check if the station is active (not deleted and is_active flag is true)
    pub fn is_active(&self) -> bool {
        !self.is_deleted() && self.is_active
    }

    /// Get the station ID
    pub fn id(&self) -> &str {
        &self.base.id
    }

    /// Get the station name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the company ID
    pub fn company_id(&self) -> &str {
        &self.company_id
    }

    /// Get the station version
    pub fn version(&self) -> i32 {
        self.version
    }

    /// Get the next version number
    pub fn next_version(&self) -> i32 {
        self.version + 1
    }

    /// Validate the station data
    pub fn validate(&self) -> Result<(), String> {
        // Validate company_id
        if !CompanyId::validate_id(&self.company_id) {
            return Err("Invalid company ID format".to_string());
        }

        // Validate name
        if self.name.trim().is_empty() {
            return Err("Station name cannot be empty".to_string());
        }
        if self.name.len() > 255 {
            return Err("Station name cannot exceed 255 characters".to_string());
        }

        // Validate address
        if self.address.trim().is_empty() {
            return Err("Station address cannot be empty".to_string());
        }

        // Validate latitude
        if self.latitude < -90.0 || self.latitude > 90.0 {
            return Err("Latitude must be between -90 and 90".to_string());
        }

        // Validate longitude
        if self.longitude < -180.0 || self.longitude > 180.0 {
            return Err("Longitude must be between -180 and 180".to_string());
        }

        // Validate email if provided
        if let Some(ref email) = self.email {
            if !email.trim().is_empty() {
                if !email.contains('@') || !email.contains('.') {
                    return Err("Invalid email format".to_string());
                }
                if email.len() > 255 {
                    return Err("Email cannot exceed 255 characters".to_string());
                }
            }
        }

        // Validate phone if provided
        if let Some(ref phone) = self.phone {
            if !phone.trim().is_empty() {
                if phone.len() > 50 {
                    return Err("Phone number cannot exceed 50 characters".to_string());
                }
            }
        }

        // Validate website if provided
        if let Some(ref website) = self.website {
            if !website.trim().is_empty() {
                if !website.starts_with("http://") && !website.starts_with("https://") {
                    return Err("Website URL must start with http:// or https://".to_string());
                }
                if website.len() > 255 {
                    return Err("Website URL cannot exceed 255 characters".to_string());
                }
            }
        }

        // Validate version
        if self.version < 1 {
            return Err("Version must be greater than or equal to 1".to_string());
        }

        Ok(())
    }
}

impl From<Station> for FullModel {
    fn from(station: Station) -> Self {
        FullModel {
            base: station.base,
            deleted_at: station.deleted_at,
            version: station.version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_new_station() {
        let company_id = CompanyId::generate_id();
        let station = Station::new(
            company_id.clone(),
            "Test Station".to_string(),
            "Test Address".to_string(),
            36.8065,
            10.1815,
        );
        
        assert_eq!(station.company_id, company_id);
        assert_eq!(station.name, "Test Station");
        assert_eq!(station.address, "Test Address");
        assert_eq!(station.latitude, 36.8065);
        assert_eq!(station.longitude, 10.1815);
        assert_eq!(station.access_type, AccessType::Public);
        assert!(station.is_active);
        assert!(!station.is_deleted());
        assert_eq!(station.version, 1);
        assert!(StationId::validate_id(&station.id));
    }

    #[test]
    fn test_create_station_with_all_fields() {
        let company_id = CompanyId::generate_id();
        let operating_hours = serde_json::json!({
            "monday": "08:00-22:00",
            "tuesday": "08:00-22:00",
            "wednesday": "08:00-22:00",
            "thursday": "08:00-22:00",
            "friday": "08:00-22:00",
            "saturday": "09:00-20:00",
            "sunday": "09:00-20:00"
        });
        
        let amenities = vec!["restroom".to_string(), "cafe".to_string(), "wifi".to_string()];
        
        let station = Station::create(
            company_id.clone(),
            "Test Station".to_string(),
            Some("Test Description".to_string()),
            "Test Address".to_string(),
            36.8065,
            10.1815,
            Some("+216-71-123-456".to_string()),
            Some("test@example.com".to_string()),
            Some("https://example.com".to_string()),
            AccessType::Public,
            Some(operating_hours),
            Some(amenities),
        );
        
        assert_eq!(station.company_id, company_id);
        assert_eq!(station.name, "Test Station");
        assert_eq!(station.description, Some("Test Description".to_string()));
        assert_eq!(station.address, "Test Address");
        assert_eq!(station.latitude, 36.8065);
        assert_eq!(station.longitude, 10.1815);
        assert_eq!(station.phone, Some("+216-71-123-456".to_string()));
        assert_eq!(station.email, Some("test@example.com".to_string()));
        assert_eq!(station.website, Some("https://example.com".to_string()));
        assert_eq!(station.access_type, AccessType::Public);
        assert_eq!(station.operating_hours, Some(operating_hours));
        assert_eq!(station.amenities, Some(amenities));
    }

    #[test]
    fn test_update_station() {
        let company_id = CompanyId::generate_id();
        let mut station = Station::new(
            company_id.clone(),
            "Test Station".to_string(),
            "Test Address".to_string(),
            36.8065,
            10.1815,
        );
        let original_version = station.version;
        
        station.update(
            Some("Updated Station".to_string()),
            Some("Updated Description".to_string()),
            Some("Updated Address".to_string()),
            Some(36.8066),
            Some(10.1816),
            Some("+216-71-654-321".to_string()),
            Some("updated@example.com".to_string()),
            Some("https://updated.com".to_string()),
            Some(AccessType::Private),
            Some(serde_json::json!({"monday": "09:00-21:00"})),
            Some(vec!["parking".to_string()]),
            Some(false),
        );
        
        assert_eq!(station.name, "Updated Station");
        assert_eq!(station.description, Some("Updated Description".to_string()));
        assert_eq!(station.address, Some("Updated Address".to_string()));
        assert_eq!(station.latitude, 36.8066);
        assert_eq!(station.longitude, 10.1816);
        assert_eq!(station.phone, Some("+216-71-654-321".to_string()));
        assert_eq!(station.email, Some("updated@example.com".to_string()));
        assert_eq!(station.website, Some("https://updated.com".to_string()));
        assert_eq!(station.access_type, AccessType::Private);
        assert!(!station.is_active);
        assert_eq!(station.version, original_version + 1);
    }

    #[test]
    fn test_soft_delete_station() {
        let company_id = CompanyId::generate_id();
        let mut station = Station::new(
            company_id,
            "Test Station".to_string(),
            "Test Address".to_string(),
            36.8065,
            10.1815,
        );
        
        assert!(!station.is_deleted());
        assert!(station.is_active());
        
        station.delete();
        
        assert!(station.is_deleted());
        assert!(!station.is_active());
        assert!(station.deleted_at.is_some());
    }

    #[test]
    fn test_restore_station() {
        let company_id = CompanyId::generate_id();
        let mut station = Station::new(
            company_id,
            "Test Station".to_string(),
            "Test Address".to_string(),
            36.8065,
            10.1815,
        );
        station.delete();
        
        assert!(station.is_deleted());
        
        station.restore();
        
        assert!(!station.is_deleted());
        assert!(station.is_active());
        assert!(station.deleted_at.is_none());
    }

    #[test]
    fn test_validate_station() {
        let company_id = CompanyId::generate_id();
        let mut station = Station::new(
            company_id,
            "Test Station".to_string(),
            "Test Address".to_string(),
            36.8065,
            10.1815,
        );
        
        // Valid station should pass validation
        assert!(station.validate().is_ok());
        
        // Invalid company ID should fail
        station.company_id = "invalid-company-id".to_string();
        assert!(station.validate().is_err());
        
        // Valid company ID should pass
        station.company_id = CompanyId::generate_id();
        assert!(station.validate().is_ok());
        
        // Empty name should fail
        station.name = "".to_string();
        assert!(station.validate().is_err());
        
        // Name too long should fail
        station.name = "a".repeat(256);
        assert!(station.validate().is_err());
        
        // Empty address should fail
        station.name = "Test Station".to_string();
        station.address = "".to_string();
        assert!(station.validate().is_err());
        
        // Invalid latitude should fail
        station.address = "Test Address".to_string();
        station.latitude = -91.0;
        assert!(station.validate().is_err());
        
        // Invalid longitude should fail
        station.latitude = 36.8065;
        station.longitude = 181.0;
        assert!(station.validate().is_err());
        
        // Invalid email should fail
        station.longitude = 10.1815;
        station.email = Some("invalid-email".to_string());
        assert!(station.validate().is_err());
        
        // Invalid website should fail
        station.email = None;
        station.website = Some("invalid-website".to_string());
        assert!(station.validate().is_err());
        
        // Invalid version should fail
        station.website = None;
        station.version = 0;
        assert!(station.validate().is_err());
    }
}