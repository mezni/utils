use serde::Deserialize;

use crate::errors::app_error::AppError;

#[derive(Debug, Deserialize)]
pub struct NearbyQuery {
    pub lat: f64,
    pub lng: f64,
    pub radius_m: f64,
}

impl NearbyQuery {
    pub fn validate(&self) -> Result<(), AppError> {
        let mut errors: Vec<String> = Vec::new();

        if self.lat < -90.0 || self.lat > 90.0 {
            errors.push(format!("lat must be between -90 and 90, got {}", self.lat));
        }
        if self.lng < -180.0 || self.lng > 180.0 {
            errors.push(format!(
                "lng must be between -180 and 180, got {}",
                self.lng
            ));
        }
        if self.radius_m <= 0.0 {
            errors.push(format!(
                "radius_m must be greater than 0, got {}",
                self.radius_m
            ));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(AppError::ValidationError(errors.join("; ")))
        }
    }
}
