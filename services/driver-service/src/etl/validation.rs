use serde::Serialize;
use std::collections::HashMap;
use crate::ingestion::tag_normalizer::TagNormalized;

/// ETL validation service
pub struct EtlValidationService;

impl EtlValidationService {
    /// Validate OSM tags against business rules
    pub fn validate_tags(&self, tags: &HashMap<String, String>) -> Result<ValidationResult, String> {
        let mut errors = Vec::new();

        // Validate amenity type
        if !tags.contains_key("amenity") {
            errors.push("Missing amenity tag".to_string());
        } else {
            let amenity = tags.get("amenity").unwrap();
            if amenity != "charging_station" && amenity != "power" {
                errors.push(format!("Invalid amenity type: {}", amenity));
            }
        }

        // Validate required tags for charging stations
        if tags.contains_key("amenity") && tags.get("amenity") == Some(&"charging_station".to_string()) {
            if !tags.contains_key("charging_station:current") && !tags.contains_key("power") {
                errors.push("Missing charging station power information".to_string());
            }
        }

        // Validate connector types
        if tags.contains_key("socket:type2") || tags.contains_key("socket:tesla_supercharger") || tags.contains_key("socket:chademo") {
            // Valid
        } else if tags.contains_key("charging_station:current") || tags.contains_key("power") {
            // Valid if power is specified
        } else {
            errors.push("Missing or invalid connector type information".to_string());
        }

        // Validate power values
        if let Some(power) = tags.get("power") {
            if !power.to_lowercase().contains("kw") && power.parse::<f64>().is_err() {
                errors.push(format!("Invalid power value: {}", power));
            }
        }

        Ok(ValidationResult {
            valid: errors.is_empty(),
            errors,
            warnings: Vec::new(),
        })
    }

    /// Validate normalized tag data
    pub fn validate_normalized(&self, normalized: &TagNormalized) -> Result<ValidationResult, String> {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        // Validate amenity
        if normalized.amenity.is_empty() {
            errors.push("Amenity cannot be empty".to_string());
        } else if normalized.amenity != "charging_station" && normalized.amenity != "power" {
            errors.push(format!("Invalid amenity type: {}", normalized.amenity));
        }

        // Validate connector types
        if normalized.connector_types.is_empty() {
            errors.push("Connector types cannot be empty".to_string());
        } else {
            // Check for unknown connector types
            for connector in &normalized.connector_types {
                if connector == "Unknown" {
                    warnings.push(format!("Unknown connector type: {}", connector));
                }
            }
        }

        // Validate power if present
        if let Some(power) = &normalized.power {
            if !power.to_lowercase().contains("kw") && power.parse::<f64>().is_err() {
                errors.push(format!("Invalid power value: {}", power));
            }
        }

        // Validate address if present
        if let Some(address) = &normalized.address {
            if address.street.is_some() || address.city.is_some() {
                // Valid
            } else {
                warnings.push("Missing address information".to_string());
            }
        }

        Ok(ValidationResult {
            valid: errors.is_empty(),
            errors,
            warnings,
        })
    }

    /// Validate coordinates
    pub fn validate_coordinates(
        &self,
        latitude: f64,
        longitude: f64,
    ) -> Result<(), String> {
        if latitude < -90.0 || latitude > 90.0 {
            return Err(format!("Invalid latitude: {}", latitude));
        }

        if longitude < -180.0 || longitude > 180.0 {
            return Err(format!("Invalid longitude: {}", longitude));
        }

        Ok(())
    }

    /// Validate connector types
    pub fn validate_connector_types(&self, types: &[String]) -> Result<(), String> {
        if types.is_empty() {
            return Err("Connector types cannot be empty".to_string());
        }

        let valid_types = vec!["AC", "DC", "Type 2", "CCS", "CHAdeMO", "Tesla Supercharger"];

        for type_name in types {
            if !valid_types.contains(&type_name.as_str()) && type_name != "Unknown" {
                return Err(format!("Invalid connector type: {}", type_name));
            }
        }

        Ok(())
    }
}

/// Validation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    pub valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

impl ValidationResult {
    pub fn new() -> Self {
        Self {
            valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }

    pub fn add_error(&mut self, error: String) {
        self.errors.push(error);
        self.valid = false;
    }

    pub fn add_warning(&mut self, warning: String) {
        self.warnings.push(warning);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_tags_valid() {
        let validator = EtlValidationService;
        let mut tags = HashMap::new();
        tags.insert("amenity".to_string(), "charging_station".to_string());
        tags.insert("power".to_string(), "50kW".to_string());
        tags.insert("socket:type2".to_string(), "1".to_string());

        let result = validator.validate_tags(&tags);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().valid, true);
    }

    #[test]
    fn test_validate_tags_missing_amenity() {
        let validator = EtlValidationService;
        let mut tags = HashMap::new();
        tags.insert("power".to_string(), "50kW".to_string());

        let result = validator.validate_tags(&tags);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().errors.len(), 1);
    }

    #[test]
    fn test_validate_tags_invalid_amenity() {
        let validator = EtlValidationService;
        let mut tags = HashMap::new();
        tags.insert("amenity".to_string(), "restaurant".to_string());

        let result = validator.validate_tags(&tags);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().errors.len(), 1);
    }

    #[test]
    fn test_validate_tags_missing_connector() {
        let validator = EtlValidationService;
        let mut tags = HashMap::new();
        tags.insert("amenity".to_string(), "charging_station".to_string());

        let result = validator.validate_tags(&tags);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().errors.len(), 1);
    }

    #[test]
    fn test_validate_coordinates_valid() {
        let validator = EtlValidationService;

        assert!(validator.validate_coordinates(40.7829, -73.9654).is_ok());
        assert!(validator.validate_coordinates(-90.0, -180.0).is_ok());
        assert!(validator.validate_coordinates(90.0, 180.0).is_ok());
    }

    #[test]
    fn test_validate_coordinates_invalid() {
        let validator = EtlValidationService;

        assert!(validator.validate_coordinates(95.0, -73.9654).is_err());
        assert!(validator.validate_coordinates(40.7829, -190.0).is_err());
        assert!(validator.validate_coordinates(-95.0, -73.9654).is_err());
    }

    #[test]
    fn test_validate_connector_types_valid() {
        let validator = EtlValidationService;

        let types = vec!["AC".to_string(), "DC".to_string(), "Type 2".to_string()];
        assert!(validator.validate_connector_types(&types).is_ok());
    }

    #[test]
    fn test_validate_connector_types_invalid() {
        let validator = EtlValidationService;

        let types = vec!["AC".to_string(), "InvalidType".to_string()];
        assert!(validator.validate_connector_types(&types).is_err());
    }
}
