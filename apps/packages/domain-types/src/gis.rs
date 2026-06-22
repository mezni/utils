use serde::{Deserialize, Serialize};

/// Station data for API responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Station {
    /// Unique station identifier (nanoid with STA- prefix)
    pub id: String,
    /// Station display name
    pub name: String,
    /// Latitude coordinate
    pub latitude: f64,
    /// Longitude coordinate
    pub longitude: f64,
    /// Distance from query point (meters)
    pub distance: Option<f64>,
    /// OSM amenity type
    pub amenity: String,
    /// Charging power (kW)
    pub power: Option<String>,
    /// Connector types (e.g., ["Type 2", "CCS"])
    pub connector_types: Option<Vec<String>>,
    /// Whether station is currently available
    pub is_available: bool,
    /// Last update timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated: Option<String>,
    /// Creation timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
}

/// Detailed station information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationDetail {
    /// Unique station identifier
    pub id: String,
    /// Station display name
    pub name: String,
    /// Latitude coordinate
    pub latitude: f64,
    /// Longitude coordinate
    pub longitude: f64,
    /// OSM amenity type
    pub amenity: String,
    /// Charging power (kW)
    pub power: Option<String>,
    /// Connector types
    pub connector_types: Option<Vec<String>>,
    /// Whether station is currently available
    pub is_available: bool,
    /// Operator name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,
    /// Address components
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<Address>,
    /// Last update timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated: Option<String>,
    /// Creation timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
}

/// Address components
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Address {
    /// Street address
    pub street: Option<String>,
    /// City
    pub city: Option<String>,
    /// State/Region
    pub state: Option<String>,
    /// Country
    pub country: Option<String>,
    /// Postal code
    pub postal_code: Option<String>,
}

/// Station list response (paginated)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationList {
    /// Station data
    pub data: Vec<Station>,
    /// Pagination metadata
    pub pagination: Pagination,
}

/// Pagination metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pagination {
    /// Current page number
    pub page: u32,
    /// Items per page
    pub limit: u32,
    /// Total items
    pub total: u64,
    /// Total pages
    pub total_pages: u32,
}

/// Search query parameters for nearby stations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NearbySearchQuery {
    /// Latitude
    pub lat: f64,
    /// Longitude
    pub lon: f64,
    /// Radius in meters
    pub radius: i32,
    /// Maximum number of results (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// Page number (for large datasets)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<u32>,
}

/// Station detail query parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationDetailQuery {
    /// Station ID
    pub id: String,
}

impl Station {
    /// Create a new Station from gis.osm_charging_stations table row
    pub fn from_db_row(
        id: String,
        name: String,
        latitude: f64,
        longitude: f64,
        amenity: String,
        power: Option<String>,
        connector_types: Option<Vec<String>>,
        is_available: bool,
        last_updated: Option<String>,
        created_at: Option<String>,
    ) -> Self {
        Self {
            id,
            name,
            latitude,
            longitude,
            distance: None,
            amenity,
            power,
            connector_types,
            is_available,
            last_updated,
            created_at,
        }
    }

    /// Update distance from query point
    pub fn with_distance(mut self, distance: f64) -> Self {
        self.distance = Some(distance);
        self
    }
}

impl StationDetail {
    /// Create a new StationDetail from gis.osm_charging_stations table row
    pub fn from_db_row(
        id: String,
        name: String,
        latitude: f64,
        longitude: f64,
        amenity: String,
        power: Option<String>,
        connector_types: Option<Vec<String>>,
        is_available: bool,
        operator: Option<String>,
        address: Option<Address>,
        last_updated: Option<String>,
        created_at: Option<String>,
    ) -> Self {
        Self {
            id,
            name,
            latitude,
            longitude,
            amenity,
            power,
            connector_types,
            is_available,
            operator,
            address,
            last_updated,
            created_at,
        }
    }

    /// Add operator information
    pub fn with_operator(mut self, operator: String) -> Self {
        self.operator = Some(operator);
        self
    }

    /// Add address information
    pub fn with_address(mut self, address: Address) -> Self {
        self.address = Some(address);
        self
    }
}

impl Pagination {
    /// Create new pagination metadata
    pub fn new(page: u32, limit: u32, total: u64) -> Self {
        let total_pages = if limit > 0 {
            let total_pages = (total / (limit as u64)).max(1);
            total_pages.try_into().unwrap_or(u32::MAX)
        } else {
            0
        };

        Self {
            page,
            limit,
            total,
            total_pages,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_station_creation() {
        let station = Station::from_db_row(
            "STA-123456789".to_string(),
            "Test Station".to_string(),
            40.7829,
            -73.9654,
            "charging_station".to_string(),
            Some("50kW".to_string()),
            Some(vec!["Type 2".to_string(), "CCS".to_string()]),
            true,
            Some("2026-06-22T10:30:00Z".to_string()),
            Some("2026-06-22T10:00:00Z".to_string()),
        );

        assert_eq!(station.id, "STA-123456789");
        assert_eq!(station.name, "Test Station");
        assert_eq!(station.latitude, 40.7829);
        assert_eq!(station.longitude, -73.9654);
        assert_eq!(station.amenity, "charging_station");
        assert_eq!(station.is_available, true);
    }

    #[test]
    fn test_station_with_distance() {
        let mut station = Station::from_db_row(
            "STA-123456789".to_string(),
            "Test Station".to_string(),
            40.7829,
            -73.9654,
            "charging_station".to_string(),
            Some("50kW".to_string()),
            Some(vec!["Type 2".to_string()]),
            true,
            None,
            None,
        );

        let station = station.with_distance(123.5);
        assert_eq!(station.distance, Some(123.5));
    }

    #[test]
    fn test_station_detail_creation() {
        let address = Address {
            street: Some("123 Main St".to_string()),
            city: Some("New York".to_string()),
            state: Some("NY".to_string()),
            country: Some("USA".to_string()),
            postal_code: Some("10001".to_string()),
        };

        let detail = StationDetail::from_db_row(
            "STA-123456789".to_string(),
            "Test Station".to_string(),
            40.7829,
            -73.9654,
            "charging_station".to_string(),
            Some("50kW".to_string()),
            Some(vec!["Type 2".to_string()]),
            true,
            Some("Tesla".to_string()),
            Some(address),
            Some("2026-06-22T10:30:00Z".to_string()),
            Some("2026-06-22T10:00:00Z".to_string()),
        );

        assert_eq!(detail.id, "STA-123456789");
        assert_eq!(detail.operator, Some("Tesla".to_string()));
        assert!(detail.address.is_some());
    }

    #[test]
    fn test_pagination_creation() {
        let pagination = Pagination::new(1, 20, 100);
        assert_eq!(pagination.page, 1);
        assert_eq!(pagination.limit, 20);
        assert_eq!(pagination.total, 100);
        assert_eq!(pagination.total_pages, 5);
    }

    #[test]
    fn test_nearby_search_query() {
        let query = NearbySearchQuery {
            lat: 40.7829,
            lon: -73.9654,
            radius: 1000,
            limit: Some(50),
            page: Some(1),
        };

        assert_eq!(query.lat, 40.7829);
        assert_eq!(query.lon, -73.9654);
        assert_eq!(query.radius, 1000);
        assert_eq!(query.limit, Some(50));
        assert_eq!(query.page, Some(1));
    }
}
