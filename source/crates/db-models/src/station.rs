use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Station {
    pub id: String,
    pub partner_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub osm_id: Option<i64>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    pub location: GeoLocation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<std::collections::HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub deleted_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoLocation {
    #[serde(rename = "type")]
    pub location_type: String,
    pub coordinates: Vec<f64>,
}

impl GeoLocation {
    pub fn new(longitude: f64, latitude: f64) -> Self {
        Self {
            location_type: "Point".to_string(),
            coordinates: vec![longitude, latitude],
        }
    }

    pub fn to_postgis(&self) -> String {
        format!("ST_SetSRID(ST_MakePoint({}, {}), 4326)", self.coordinates[0], self.coordinates[1])
    }
}

impl From<GeoLocation> for GeoLocation {
    fn from(geo: GeoLocation) -> Self {
        geo
    }
}

impl From<crate::CreateStationRequest> for GeoLocation {
    fn from(request: crate::CreateStationRequest) -> Self {
        GeoLocation {
            location_type: request.location.location_type,
            coordinates: request.location.coordinates,
        }
    }
}

impl Station {
    pub fn new(id: String, partner_id: String, name: String, location: GeoLocation) -> Self {
        Self {
            id,
            partner_id,
            osm_id: None,
            name,
            address: None,
            location,
            tags: None,
            created_by: None,
            updated_by: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            deleted_at: None,
        }
    }

    pub fn update(&mut self, name: String, address: Option<String>, location: Option<GeoLocation>, tags: Option<std::collections::HashMap<String, String>>, updated_by: Option<String>) {
        self.name = name;
        self.address = address;
        self.location = location.unwrap_or_else(|| self.location.clone());
        self.tags = tags;
        self.updated_by = updated_by;
        self.updated_at = chrono::Utc::now();
    }

    pub fn soft_delete(&mut self, deleted_at: Option<chrono::DateTime<chrono::Utc>>) {
        self.deleted_at = deleted_at;
        self.updated_at = chrono::Utc::now();
    }

    pub fn is_active(&self) -> bool {
        self.deleted_at.is_none()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateStationRequest {
    pub partner_id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    pub location: GeoLocation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub osm_id: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateStationRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<GeoLocation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StationResponse {
    pub id: String,
    pub partner_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub osm_id: Option<i64>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    pub location: GeoLocation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<std::collections::HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<Station> for StationResponse {
    fn from(station: Station) -> Self {
        Self {
            id: station.id,
            partner_id: station.partner_id,
            osm_id: station.osm_id,
            name: station.name,
            address: station.address,
            location: station.location,
            tags: station.tags,
            created_by: station.created_by,
            updated_by: station.updated_by,
            created_at: station.created_at.to_rfc3339(),
            updated_at: station.updated_at.to_rfc3339(),
        }
    }
}
