use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoPoint {
    pub lat: f64,
    pub lng: f64,
}

impl GeoPoint {
    pub fn new(lat: f64, lng: f64) -> Result<Self, crate::CoreError> {
        if !(-90.0..=90.0).contains(&lat) {
            return Err(crate::CoreError::Validation(format!(
                "lat must be between -90 and 90, got {}",
                lat
            )));
        }
        if !(-180.0..=180.0).contains(&lng) {
            return Err(crate::CoreError::Validation(format!(
                "lng must be between -180 and 180, got {}",
                lng
            )));
        }
        Ok(GeoPoint { lat, lng })
    }
}
