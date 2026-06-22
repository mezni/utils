use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::ingestion::osm_parser::{OsmParser, OsmTagNormalized, Address};

/// Tag normalizer for OSM to internal schema mapping
pub struct TagNormalizer;

impl TagNormalizer {
    /// Normalize OSM tags to internal schema fields
    pub fn normalize(&self, tags: &HashMap<String, String>) -> TagNormalized {
        let amenity = self.normalize_amenity(tags.get("amenity"));
        let power = self.normalize_power(tags.get("power"));
        let name = tags.get("name").cloned().unwrap_or_default();
        let operator = tags.get("operator").cloned();
        let address = self.normalize_address(tags);
        let connector_types = self.normalize_connector_types(tags);

        TagNormalized {
            amenity,
            power,
            name,
            operator,
            address,
            connector_types,
            tags: tags.clone(),
        }
    }

    /// Normalize amenity type
    fn normalize_amenity(&self, amenity: Option<&String>) -> String {
        amenity
            .map(|a| a.to_lowercase())
            .unwrap_or_default()
    }

    /// Normalize power value
    fn normalize_power(&self, power: Option<&String>) -> Option<String> {
        if let Some(p) = power {
            // Extract numeric value if it contains kW
            let clean = p.trim().to_lowercase();

            if clean.contains("kw") {
                Some(p.clone())
            } else if let Ok(num) = p.parse::<f64>() {
                Some(format!("{}kW", num))
            } else {
                Some(p.clone())
            }
        } else {
            None
        }
    }

    /// Normalize address components
    fn normalize_address(&self, tags: &HashMap<String, String>) -> Option<Address> {
        let mut address = Address::default();

        // Extract address components with fallback to addr: prefix
        if let Some(street) = tags.get("street").or_else(|| tags.get("addr:street")) {
            address.street = Some(street.clone());
        }

        if let Some(city) = tags.get("city").or_else(|| tags.get("addr:city")) {
            address.city = Some(city.clone());
        }

        if let Some(state) = tags.get("state").or_else(|| tags.get("addr:state")) {
            address.state = Some(state.clone());
        }

        if let Some(country) = tags.get("country").or_else(|| tags.get("addr:country")) {
            address.country = Some(country.clone());
        }

        if let Some(postal_code) = tags.get("postcode").or_else(|| tags.get("addr:postcode")) {
            address.postal_code = Some(postal_code.clone());
        }

        if address.street.is_some() || address.city.is_some() {
            Some(address)
        } else {
            None
        }
    }

    /// Normalize connector types
    fn normalize_connector_types(&self, tags: &HashMap<String, String>) -> Vec<String> {
        let mut connectors = Vec::new();

        // Extract from charging_station tags
        if let Some(current) = tags.get("charging_station:current") {
            connectors.extend(self.current_to_connector_type(current));
        }

        if let Some(voltage) = tags.get("charging_station:voltage") {
            connectors.extend(self.voltage_to_connector_type(voltage));
        }

        // Add known connector types
        if tags.contains_key("socket:type2") {
            connectors.push("Type 2".to_string());
        }

        if tags.contains_key("socket:tesla_supercharger") {
            connectors.push("Tesla Supercharger".to_string());
        }

        if tags.contains_key("socket:chademo") {
            connectors.push("CHAdeMO".to_string());
        }

        if tags.contains_key("socket:ccs") {
            connectors.push("CCS".to_string());
        }

        // Extract from network tag
        if let Some(network) = tags.get("network") {
            connectors.push(format!("Network: {}", network));
        }

        if connectors.is_empty() {
            connectors.push("Unknown".to_string());
        }

        connectors
    }

    /// Convert current type to connector type
    fn current_to_connector_type(&self, current: &str) -> Vec<String> {
        match current.to_lowercase().as_str() {
            "ac" | "ac/dc" => vec!["AC".to_string(), "Type 2".to_string()],
            "dc" | "dc/ac" => vec!["DC".to_string(), "CCS".to_string()],
            _ => vec!["Unknown".to_string()],
        }
    }

    /// Convert voltage to connector type
    fn voltage_to_connector_type(&self, voltage: &str) -> Vec<String> {
        match voltage.to_lowercase().as_str() {
            "230" => vec!["AC".to_string()],
            "400" => vec!["AC".to_string()],
            "480" => vec!["DC".to_string()],
            "920" => vec!["DC".to_string()],
            "1000" => vec!["DC".to_string()],
            "500" => vec!["DC".to_string()],
            _ => vec!["Unknown".to_string()],
        }
    }
}

/// Normalized tag data matching internal schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagNormalized {
    pub amenity: String,
    pub power: Option<String>,
    pub name: String,
    pub operator: Option<String>,
    pub address: Option<Address>,
    pub connector_types: Vec<String>,
    pub tags: HashMap<String, String>,
}

impl TagNormalized {
    /// Validate the normalized data
    pub fn validate(&self) -> Result<(), String> {
        // Validate amenity
        if self.amenity.is_empty() {
            return Err("Amenity cannot be empty".to_string());
        }

        // Validate connector types
        if self.connector_types.is_empty() {
            return Err("Connector types cannot be empty".to_string());
        }

        // Validate connector types are valid
        for connector in &self.connector_types {
            if connector == "Unknown" {
                return Err(format!("Invalid connector type: {}", connector));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_amenity() {
        let normalizer = TagNormalizer;
        assert_eq!(normalizer.normalize_amenity(Some(&"charging_station".to_string())), "charging_station");
        assert_eq!(normalizer.normalize_amenity(Some(&"power".to_string())), "power");
        assert_eq!(normalizer.normalize_amenity(None), "");
    }

    #[test]
    fn test_normalize_power() {
        let normalizer = TagNormalizer;
        assert_eq!(normalizer.normalize_power(Some(&"50kW".to_string())), Some("50kW".to_string()));
        assert_eq!(normalizer.normalize_power(Some(&"DC".to_string())), Some("DC".to_string()));
        assert_eq!(normalizer.normalize_power(Some(&"7kW".to_string())), Some("7kW".to_string()));
    }

    #[test]
    fn test_normalize_power_with_numbers() {
        let normalizer = TagNormalizer;
        assert_eq!(normalizer.normalize_power(Some(&"50".to_string())), Some("50kW".to_string()));
    }

    #[test]
    fn test_normalize_address() {
        let normalizer = TagNormalizer;
        let mut tags = HashMap::new();
        tags.insert("street".to_string(), "123 Main St".to_string());
        tags.insert("city".to_string(), "New York".to_string());

        let normalized = normalizer.normalize(&tags);
        assert!(normalized.address.is_some());
        assert_eq!(normalized.address.as_ref().unwrap().street, Some("123 Main St".to_string()));
        assert_eq!(normalized.address.as_ref().unwrap().city, Some("New York".to_string()));
    }

    #[test]
    fn test_normalize_connector_types() {
        let normalizer = TagNormalizer;
        let mut tags = HashMap::new();
        tags.insert("amenity".to_string(), "charging_station".to_string());
        tags.insert("charging_station:current".to_string(), "AC".to_string());

        let normalized = normalizer.normalize(&tags);
        assert!(normalized.connector_types.contains(&"AC".to_string()));
        assert!(normalized.connector_types.contains(&"Type 2".to_string()));
    }

    #[test]
    fn test_validate_success() {
        let normalizer = TagNormalizer;
        let mut tags = HashMap::new();
        tags.insert("amenity".to_string(), "charging_station".to_string());
        tags.insert("power".to_string(), "50kW".to_string());

        let normalized = normalizer.normalize(&tags);
        assert!(normalized.validate().is_ok());
    }

    #[test]
    fn test_validate_empty_amenity() {
        let normalizer = TagNormalizer;
        let mut tags = HashMap::new();
        tags.insert("amenity".to_string(), "".to_string());

        let normalized = normalizer.normalize(&tags);
        assert!(normalized.validate().is_err());
    }

    #[test]
    fn test_validate_empty_connector_types() {
        let normalizer = TagNormalizer;
        let mut tags = HashMap::new();
        tags.insert("amenity".to_string(), "charging_station".to_string());
        tags.insert("power".to_string(), "50kW".to_string());
        tags.remove("socket:type2");
        tags.remove("socket:tesla_supercharger");
        tags.remove("socket:chademo");

        let normalized = normalizer.normalize(&tags);
        assert!(normalized.validate().is_err());
    }
}
