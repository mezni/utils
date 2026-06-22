use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// OSM tag data extracted from XML
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OsmTag {
    pub osm_id: i64,
    pub tags: std::collections::HashMap<String, String>,
    pub position: Option<(f64, f64)>, // (lat, lon)
}

/// Ingestion job status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum IngestionJobStatus {
    Pending,
    Processing,
    Completed,
    Failed,
}

/// Ingestion job result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionJob {
    pub job_id: String,
    pub status: IngestionJobStatus,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub station_count: Option<u64>,
    pub error_message: Option<String>,
}

/// Ingestion service for OSM data
pub struct OsmIngestionService {
    client: Client,
    /// Overpass API endpoint
    overpass_url: String,
    /// Batch size for API requests
    batch_size: usize,
    /// Timeout for API requests
    timeout: Duration,
}

impl OsmIngestionService {
    /// Create a new OSM ingestion service
    pub fn new() -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("Failed to create HTTP client"),
            overpass_url: "https://overpass-api.de/api/interpreter".to_string(),
            batch_size: 100,
            timeout: Duration::from_secs(30),
        }
    }

    /// Create a new OSM ingestion service with custom configuration
    pub fn with_config(
        overpass_url: String,
        batch_size: usize,
        timeout_secs: u64,
    ) -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(timeout_secs))
                .build()
                .expect("Failed to create HTTP client"),
            overpass_url,
            batch_size,
            timeout: Duration::from_secs(timeout_secs),
        }
    }

    /// Fetch OSM data using Overpass API
    pub async fn fetch_osm_data(&self, query: &str) -> Result<String, OsmError> {
        let url = format!("{}?data={}", self.overpass_url, query);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| OsmError::HttpError(e.to_string()))?;

        if !response.status().is_success() {
            return Err(OsmError::ApiError(
                response.status().to_string(),
                "Overpass API request failed".to_string(),
            ));
        }

        let text = response
            .text()
            .await
            .map_err(|e| OsmError::ParseError(e.to_string()))?;

        Ok(text)
    }

    /// Parse OSM XML and extract charging station data
    pub fn parse_osm_xml(&self, xml: &str) -> Result<Vec<OsmTag>, OsmError> {
        // Simple XML parsing for demonstration
        // In production, would use proper XML parser or XML-to-JSON conversion

        let mut stations = Vec::new();

        // Split by <node> tags (simplified parsing)
        for node_xml in xml.split("<node") {
            if node_xml.starts_with(" id=") {
                let node = node_xml.trim().to_string();
                if let Some(parsed) = self.parse_single_node(&node) {
                    stations.push(parsed);
                }
            }
        }

        // Validate and process stations
        let mut valid_stations = Vec::new();
        for station in stations {
            // Check if node has charging station tags
            if self.has_charging_station_tags(&station.tags) {
                valid_stations.push(station);
            }
        }

        Ok(valid_stations)
    }

    /// Parse a single OSM node
    fn parse_single_node(&self, node_xml: &str) -> Option<OsmTag> {
        // Extract attributes using regex (simplified)
        let id: i64 = node_xml
            .split(" id=\"")
            .nth(1)?
            .split('"')
            .next()?
            .parse()
            .ok()?;

        let mut tags = std::collections::HashMap::new();

        // Extract tags
        for tag in node_xml.split("<tag k=") {
            if let Some(tag_str) = tag.trim().strip_prefix(" k=") {
                if let Some((key, value)) = tag_str.split_once('\"') {
                    tags.insert(key.trim().to_string(), value.trim().to_string());
                }
            }
        }

        // Extract position
        let position: Option<(f64, f64)> = node_xml
            .split(" lat=\"")
            .nth(1)?
            .split('"')
            .nth(0)?
            .parse()
            .ok()
            .and_then(|lat: f64| {
                node_xml.split(" lon=\"").nth(1)?.split('"').nth(0)?.parse().ok()
            });

        Some(OsmTag {
            osm_id: id,
            tags,
            position,
        })
    }

    /// Check if node has charging station related tags
    fn has_charging_station_tags(&self, tags: &std::collections::HashMap<String, String>) -> bool {
        // Check for common charging station amenity tags
        tags
            .contains_key("amenity")
            && (tags.get("amenity") == Some(&"charging_station".to_string())
                || tags.get("amenity") == Some(&"power".to_string()))
    }

    /// Generate an idempotency key for OSM data
    pub fn generate_idempotency_key(&self, osm_id: i64) -> String {
        format!("osm:ingest:{}", osm_id)
    }

    /// Create a batch query for Overpass API
    pub fn create_batch_query(&self, bounding_box: &BoundingBoxQuery) -> String {
        format!(
            "area[name='BorneMap']->.searchArea;way(area.searchArea)[amenity~'charging_station|power'];(._;>;);out;",
        )
    }
}

/// OSM ingestion errors
#[derive(Debug, thiserror::Error)]
pub enum OsmError {
    #[error("HTTP error: {0}")]
    HttpError(String),

    #[error("API error: status={}, message={}", status, message)]
    ApiError(String, String),

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Validation error: {0}")]
    ValidationError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_creation() {
        let service = OsmIngestionService::new();
        assert_eq!(service.batch_size, 100);
    }

    #[test]
    fn test_service_with_config() {
        let service = OsmIngestionService::with_config(
            "https://custom.overpass-api.de/api/interpreter".to_string(),
            200,
            60,
        );
        assert_eq!(service.batch_size, 200);
        assert_eq!(
            service.overpass_url,
            "https://custom.overpass-api.de/api/interpreter"
        );
    }

    #[test]
    fn test_idempotency_key_generation() {
        let service = OsmIngestionService::new();
        let key = service.generate_idempotency_key(123456789);
        assert_eq!(key, "osm:ingest:123456789");
    }

    #[test]
    fn test_parse_osm_xml_empty() {
        let service = OsmIngestionService::new();
        let result = service.parse_osm_xml("<osm></osm>");
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_parse_osm_xml_single_node() {
        let service = OsmIngestionService::new();
        let xml = r#"<osm><node id="123" lat="40.7128" lon="-74.0060">
            <tag k="amenity" v="charging_station"/>
            <tag k="name" v="Central Park"/>
            <tag k="operator" v="Tesla"/>
        </node></osm>"#;

        let result = service.parse_osm_xml(xml);
        assert!(result.is_ok());
        let stations = result.unwrap();
        assert_eq!(stations.len(), 1);
        assert_eq!(stations[0].osm_id, 123);
        assert_eq!(stations[0].tags.get("amenity"), Some(&"charging_station".to_string()));
        assert_eq!(stations[0].tags.get("name"), Some(&"Central Park".to_string()));
    }

    #[test]
    fn test_parse_osm_xml_no_charging_stations() {
        let service = OsmIngestionService::new();
        let xml = r#"<osm><node id="123" lat="40.7128" lon="-74.0060">
            <tag k="amenity" v="restaurant"/>
        </node></osm>"#;

        let result = service.parse_osm_xml(xml);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_has_charging_station_tags() {
        let service = OsmIngestionService::new();
        let tags = std::collections::HashMap::new();

        assert!(!service.has_charging_station_tags(&tags));

        tags.insert("amenity".to_string(), "charging_station".to_string());
        assert!(service.has_charging_station_tags(&tags));

        tags.insert("amenity".to_string(), "restaurant".to_string());
        assert!(!service.has_charging_station_tags(&tags));
    }
}
