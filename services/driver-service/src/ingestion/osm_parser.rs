use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::telemetry::ingestion::{OsmIngestionService, OsmTag, OsmError};

/// OSM XML parser for extracting charging station data
pub struct OsmParser;

impl OsmParser {
    /// Parse OSM XML and extract charging station data
    pub fn parse_osm_xml(&self, xml: &str) -> Result<Vec<OsmTag>, OsmError> {
        let mut stations = Vec::new();

        // Split by <node> tags
        for node_xml in xml.split("<node") {
            if node_xml.starts_with(" id=") {
                let node = node_xml.trim().to_string();
                if let Some(parsed) = self.parse_single_node(&node) {
                    stations.push(parsed);
                }
            }
        }

        // Filter for charging station data
        let valid_stations: Vec<_> = stations
            .into_iter()
            .filter(|station| self.has_charging_station_tags(&station.tags))
            .collect();

        Ok(valid_stations)
    }

    /// Parse a single OSM node
    fn parse_single_node(&self, node_xml: &str) -> Option<OsmTag> {
        // Extract attributes using regex-like string parsing
        let id: i64 = node_xml
            .split(" id=\"")
            .nth(1)?
            .split('"')
            .next()?
            .parse()
            .ok()?;

        let mut tags = HashMap::new();

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
    fn has_charging_station_tags(&self, tags: &HashMap<String, String>) -> bool {
        tags
            .contains_key("amenity")
            && (tags.get("amenity") == Some(&"charging_station".to_string())
                || tags.get("amenity") == Some(&"power".to_string()))
    }

    /// Normalize tags to internal schema
    pub fn normalize_tags(&self, tags: &HashMap<String, String>) -> OsmTagNormalized {
        let amenity = tags.get("amenity").cloned().unwrap_or_default();
        let power = tags.get("power").cloned();
        let name = tags.get("name").cloned().unwrap_or_default();

        // Extract connector types
        let connector_types = self.extract_connector_types(tags);

        // Extract address
        let address = self.extract_address(tags);

        OsmTagNormalized {
            amenity,
            power,
            name,
            connector_types,
            address,
            tags: tags.clone(),
        }
    }

    /// Extract connector types from tags
    fn extract_connector_types(&self, tags: &HashMap<String, String>) -> Vec<String> {
        let mut connectors = Vec::new();

        // Check for charging station tags
        if tags.contains_key("charging_station:current") {
            if let Some(current) = tags.get("charging_station:current") {
                connectors.extend(self.current_to_connector_type(current));
            }
        }

        if tags.contains_key("network") {
            connectors.push(format!("Network: {}", tags.get("network").unwrap()));
        }

        // Add other known connector types
        if tags.contains_key("socket:type2") {
            connectors.push("Type 2".to_string());
        }

        if tags.contains_key("socket:tesla") {
            connectors.push("Tesla Supercharger".to_string());
        }

        if connectors.is_empty() {
            connectors.push("Unknown".to_string());
        }

        connectors
    }

    /// Convert current type to connector type
    fn current_to_connector_type(&self, current: &str) -> Vec<String> {
        match current {
            "AC" | "AC/DC" => vec!["AC".to_string(), "Type 2".to_string()],
            "DC" | "DC/AC" => vec!["DC".to_string(), "CCS".to_string()],
            _ => vec!["Unknown".to_string()],
        }
    }

    /// Extract address from tags
    fn extract_address(&self, tags: &HashMap<String, String>) -> Option<Address> {
        let mut address = Address::default();

        // Extract address components
        if let Some(street) = tags.get("addr:street") {
            address.street = Some(street.clone());
        }

        if let Some(city) = tags.get("addr:city") {
            address.city = Some(city.clone());
        }

        if let Some(state) = tags.get("addr:state") {
            address.state = Some(state.clone());
        }

        if let Some(country) = tags.get("addr:country") {
            address.country = Some(country.clone());
        }

        if let Some(postal_code) = tags.get("addr:postcode") {
            address.postal_code = Some(postal_code.clone());
        }

        if address.street.is_some() || address.city.is_some() {
            Some(address)
        } else {
            None
        }
    }
}

/// Normalized OSM tag data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OsmTagNormalized {
    pub amenity: String,
    pub power: Option<String>,
    pub name: String,
    pub connector_types: Vec<String>,
    pub address: Option<Address>,
    pub tags: HashMap<String, String>,
}

/// Address components
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Address {
    pub street: Option<String>,
    pub city: Option<String>,
    pub state: Option<String>,
    pub country: Option<String>,
    pub postal_code: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_osm_xml_empty() {
        let parser = OsmParser;
        let result = parser.parse_osm_xml("<osm></osm>");
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_parse_osm_xml_single_charging_station() {
        let parser = OsmParser;
        let xml = r#"<osm><node id="123" lat="40.7128" lon="-74.0060">
            <tag k="amenity" v="charging_station"/>
            <tag k="name" v="Central Park"/>
            <tag k="operator" v="Tesla"/>
            <tag k="power" v="DC"/>
            <tag k="addr:street" v="Main St"/>
            <tag k="addr:city" v="New York"/>
        </node></osm>"#;

        let result = parser.parse_osm_xml(xml);
        assert!(result.is_ok());
        let stations = result.unwrap();
        assert_eq!(stations.len(), 1);
        assert_eq!(stations[0].osm_id, 123);
        assert_eq!(stations[0].tags.get("amenity"), Some(&"charging_station".to_string()));
        assert_eq!(stations[0].tags.get("name"), Some(&"Central Park".to_string()));
    }

    #[test]
    fn test_parse_osm_xml_no_charging_stations() {
        let parser = OsmParser;
        let xml = r#"<osm><node id="123" lat="40.7128" lon="-74.0060">
            <tag k="amenity" v="restaurant"/>
        </node></osm>"#;

        let result = parser.parse_osm_xml(xml);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_has_charging_station_tags() {
        let parser = OsmParser;
        let tags = HashMap::new();

        assert!(!parser.has_charging_station_tags(&tags));

        tags.insert("amenity".to_string(), "charging_station".to_string());
        assert!(parser.has_charging_station_tags(&tags));

        tags.insert("amenity".to_string(), "restaurant".to_string());
        assert!(!parser.has_charging_station_tags(&tags));
    }

    #[test]
    fn test_normalize_tags() {
        let parser = OsmParser;
        let mut tags = HashMap::new();
        tags.insert("amenity".to_string(), "charging_station".to_string());
        tags.insert("power".to_string(), "DC".to_string());
        tags.insert("name".to_string(), "Test Station".to_string());

        let normalized = parser.normalize_tags(&tags);
        assert_eq!(normalized.amenity, "charging_station");
        assert_eq!(normalized.power, Some("DC".to_string()));
        assert_eq!(normalized.name, "Test Station");
        assert!(!normalized.connector_types.is_empty());
    }

    #[test]
    fn test_extract_connector_types() {
        let parser = OsmParser;
        let mut tags = HashMap::new();
        tags.insert("amenity".to_string(), "charging_station".to_string());
        tags.insert("charging_station:current".to_string(), "AC".to_string());

        let normalized = parser.normalize_tags(&tags);
        assert!(normalized.connector_types.contains(&"AC".to_string()));
        assert!(normalized.connector_types.contains(&"Type 2".to_string()));
    }
}
