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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_query() {
        let q = NearbyQuery {
            lat: 36.8065,
            lng: 10.1815,
            radius_m: 50000.0,
        };
        assert!(q.validate().is_ok());
    }

    #[test]
    fn lat_below_range() {
        let q = NearbyQuery {
            lat: -91.0,
            lng: 0.0,
            radius_m: 1000.0,
        };
        let err = q.validate().unwrap_err();
        assert!(err.to_string().contains("lat must be between"));
        assert!(matches!(err, AppError::ValidationError(_)));
    }

    #[test]
    fn lat_above_range() {
        let q = NearbyQuery {
            lat: 91.0,
            lng: 0.0,
            radius_m: 1000.0,
        };
        let err = q.validate().unwrap_err();
        assert!(err.to_string().contains("lat must be between"));
    }

    #[test]
    fn lng_below_range() {
        let q = NearbyQuery {
            lat: 0.0,
            lng: -181.0,
            radius_m: 1000.0,
        };
        let err = q.validate().unwrap_err();
        assert!(err.to_string().contains("lng must be between"));
    }

    #[test]
    fn lng_above_range() {
        let q = NearbyQuery {
            lat: 0.0,
            lng: 181.0,
            radius_m: 1000.0,
        };
        let err = q.validate().unwrap_err();
        assert!(err.to_string().contains("lng must be between"));
    }

    #[test]
    fn radius_must_be_positive() {
        let q = NearbyQuery {
            lat: 0.0,
            lng: 0.0,
            radius_m: 0.0,
        };
        let err = q.validate().unwrap_err();
        assert!(err.to_string().contains("radius_m must be greater than 0"));
    }

    #[test]
    fn radius_must_be_positive_negative() {
        let q = NearbyQuery {
            lat: 0.0,
            lng: 0.0,
            radius_m: -1.0,
        };
        let err = q.validate().unwrap_err();
        assert!(err.to_string().contains("radius_m must be greater than 0"));
    }

    #[test]
    fn multiple_errors_collected() {
        let q = NearbyQuery {
            lat: 100.0,
            lng: 200.0,
            radius_m: 0.0,
        };
        let err = q.validate().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("lat must be between"));
        assert!(msg.contains("lng must be between"));
        assert!(msg.contains("radius_m must be greater than 0"));
    }

    #[test]
    fn boundary_values_valid() {
        let q = NearbyQuery {
            lat: 90.0,
            lng: 180.0,
            radius_m: 1.0,
        };
        assert!(q.validate().is_ok());

        let q = NearbyQuery {
            lat: -90.0,
            lng: -180.0,
            radius_m: 999999.0,
        };
        assert!(q.validate().is_ok());
    }
}
