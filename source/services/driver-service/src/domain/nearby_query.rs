use std::f64::consts::PI;

#[derive(Debug, Clone)]
pub struct NearbyQuery {
    lat: f64,
    lng: f64,
    radius_meters: f64,
}

impl NearbyQuery {
    pub fn new(lat: f64, lng: f64, radius_meters: f64) -> Result<Self, ValidationError> {
        if !(-90.0..=90.0).contains(&lat) {
            return Err(ValidationError::LatitudeOutOfRange(lat));
        }
        if !(-180.0..=180.0).contains(&lng) {
            return Err(ValidationError::LongitudeOutOfRange(lng));
        }
        if radius_meters <= 0.0 {
            return Err(ValidationError::RadiusMustBePositive(radius_meters));
        }
        if radius_meters > 200_000.0 {
            return Err(ValidationError::RadiusTooLarge(radius_meters));
        }

        Ok(Self { lat, lng, radius_meters })
    }

    pub fn lat(&self) -> f64 { self.lat }
    pub fn lng(&self) -> f64 { self.lng }
    pub fn radius_meters(&self) -> f64 { self.radius_meters }
}

#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("latitude {0} out of range [-90, 90]")]
    LatitudeOutOfRange(f64),
    #[error("longitude {0} out of range [-180, 180]")]
    LongitudeOutOfRange(f64),
    #[error("radius {0} must be positive")]
    RadiusMustBePositive(f64),
    #[error("radius {0} exceeds maximum 200 km")]
    RadiusTooLarge(f64),
}
