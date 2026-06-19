use validator::Validate;

#[derive(Debug, Clone, Validate)]
pub struct StationValidation {
    pub name: String,
    pub location: GeoLocation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub osm_id: Option<i64>,
}

#[derive(Debug, Clone, Validate)]
pub struct GeoLocation {
    #[serde(rename = "type")]
    pub location_type: String,
    pub coordinates: Vec<f64>,
}

impl GeoLocation {
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        if self.location_type != "Point" {
            errors.push(format!("invalid location format. Must be GeoJSON Point with type 'Point'"));
        }

        if self.coordinates.len() != 2 {
            errors.push(format!("invalid location format. Must have 2 coordinates [longitude, latitude]"));
        } else {
            // Coordinates should be valid longitude/latitude
            if self.coordinates[0] < -180.0 || self.coordinates[0] > 180.0 {
                errors.push("longitude must be between -180 and 180".to_string());
            }
            if self.coordinates[1] < -90.0 || self.coordinates[1] > 90.0 {
                errors.push("latitude must be between -90 and 90".to_string());
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

impl StationValidation {
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        if self.name.is_empty() {
            errors.push("name is required".to_string());
        } else if self.name.len() > 255 {
            errors.push("name must be less than 255 characters".to_string());
        }

        let geo_validation = self.location.validate();
        if geo_validation.is_err() {
            errors.extend(geo_validation.unwrap_err());
        }

        if let Some(address) = &self.address {
            if address.len() > 1000 {
                errors.push("address must be less than 1000 characters".to_string());
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

pub fn validate_station(request: &crate::CreateStationRequest) -> Result<(), Vec<String>> {
    let validation = StationValidation {
        name: request.name.clone(),
        location: request.location.clone(),
        address: request.address.clone(),
        osm_id: request.osm_id,
    };

    validation.validate()
}
