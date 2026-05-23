use serde::{Deserialize, Serialize};
use validator::Validate;

#[derive(Debug, Serialize, Deserialize, Validate)]
pub struct CreateStationRequest {
    #[validate(length(min = 1, max = 255, message = "Station name must be between 1 and 255 characters"))]
    pub name: String,
    
    #[serde(default)]
    pub description: Option<String>,
    
    #[validate(length(min = 1, message = "Station address is required"))]
    pub address: String,
    
    #[validate(range(min = -90.0, max = 90.0, message = "Latitude must be between -90 and 90"))]
    pub latitude: f64,
    
    #[validate(range(min = -180.0, max = 180.0, message = "Longitude must be between -180 and 180"))]
    pub longitude: f64,
    
    pub company_id: String,
    
    #[serde(default)]
    pub phone: Option<String>,
    
    #[serde(default)]
    #[validate(email(message = "Invalid email format"))]
    pub email: Option<String>,
    
    #[serde(default)]
    pub website: Option<String>,
    
    #[serde(default)]
    pub access_type: Option<String>,
    
    #[serde(default)]
    pub operating_hours: Option<serde_json::Value>,
    
    #[serde(default)]
    pub amenities: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize, Validate)]
pub struct UpdateStationRequest {
    #[validate(length(min = 1, max = 255, message = "Station name must be between 1 and 255 characters"))]
    pub name: Option<String>,
    
    #[serde(default)]
    pub description: Option<String>,
    
    #[validate(length(min = 1, message = "Station address is required"))]
    pub address: Option<String>,
    
    #[validate(range(min = -90.0, max = 90.0, message = "Latitude must be between -90 and 90"))]
    pub latitude: Option<f64>,
    
    #[validate(range(min = -180.0, max = 180.0, message = "Longitude must be between -180 and 180"))]
    pub longitude: Option<f64>,
    
    #[serde(default)]
    pub phone: Option<String>,
    
    #[serde(default)]
    #[validate(email(message = "Invalid email format"))]
    pub email: Option<String>,
    
    #[serde(default)]
    pub website: Option<String>,
    
    #[serde(default)]
    pub access_type: Option<String>,
    
    #[serde(default)]
    pub operating_hours: Option<serde_json::Value>,
    
    #[serde(default)]
    pub amenities: Option<Vec<String>>,
    
    #[serde(default)]
    pub is_active: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StationResponse {
    pub id: String,
    pub company_id: String,
    pub name: String,
    pub description: Option<String>,
    pub address: String,
    pub latitude: f64,
    pub longitude: f64,
    pub phone: Option<String>,
    pub email: Option<String>,
    pub website: Option<String>,
    pub access_type: String,
    pub operating_hours: Option<serde_json::Value>,
    pub amenities: Option<Vec<String>>,
    pub is_active: bool,
    pub version: i32,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<crate::models::Station> for StationResponse {
    fn from(station: crate::models::Station) -> Self {
        Self {
            id: station.base.id,
            company_id: station.company_id,
            name: station.name,
            description: station.description,
            address: station.address,
            latitude: station.latitude,
            longitude: station.longitude,
            phone: station.phone,
            email: station.email,
            website: station.website,
            access_type: station.access_type.to_string(),
            operating_hours: station.operating_hours,
            amenities: station.amenities,
            is_active: station.is_active,
            version: station.version,
            created_at: station.base.created_at,
            updated_at: station.base.updated_at,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StationListResponse {
    pub stations: Vec<StationResponse>,
    pub total: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NearbyStationRequest {
    pub latitude: f64,
    pub longitude: f64,
    #[serde(default = "default_radius")]
    pub radius: f64,
}

fn default_radius() -> f64 {
    5.0 // 5km default radius
}