use serde::{Deserialize, Serialize};

/// Spatial query parameters for radius search
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RadiusSearchQuery {
    pub latitude: f64,
    pub longitude: f64,
    pub radius_meters: i32,
}

impl RadiusSearchQuery {
    /// Validate the search query parameters
    pub fn validate(&self) -> Result<(), String> {
        // Validate latitude range
        if self.latitude < -90.0 || self.latitude > 90.0 {
            return Err(format!(
                "Latitude must be between -90 and 90, got: {}",
                self.latitude
            ));
        }

        // Validate longitude range
        if self.longitude < -180.0 || self.longitude > 180.0 {
            return Err(format!(
                "Longitude must be between -180 and 180, got: {}",
                self.longitude
            ));
        }

        // Validate radius range (minimum 100m, maximum 100km)
        if self.radius_meters < 100 {
            return Err(format!(
                "Radius must be at least 100 meters, got: {}",
                self.radius_meters
            ));
        }

        if self.radius_meters > 100000 {
            return Err(format!(
                "Radius must be at most 100000 meters (100km), got: {}",
                self.radius_meters
            ));
        }

        Ok(())
    }
}

/// Bounding box query parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoundingBoxQuery {
    pub min_lat: f64,
    pub max_lat: f64,
    pub min_lon: f64,
    pub max_lon: f64,
    pub radius_meters: Option<i32>,
}

impl BoundingBoxQuery {
    /// Validate the bounding box query parameters
    pub fn validate(&self) -> Result<(), String> {
        // Validate latitude ranges
        if self.min_lat < -90.0 || self.max_lat > 90.0 {
            return Err(format!(
                "Latitude range invalid: min={}, max={}",
                self.min_lat, self.max_lat
            ));
        }

        if self.min_lat >= self.max_lat {
            return Err(format!(
                "Invalid latitude range: min_lat ({}) must be less than max_lat ({})",
                self.min_lat, self.max_lat
            ));
        }

        // Validate longitude ranges
        if self.min_lon < -180.0 || self.max_lon > 180.0 {
            return Err(format!(
                "Longitude range invalid: min={}, max={}",
                self.min_lon, self.max_lon
            ));
        }

        if self.min_lon >= self.max_lon {
            return Err(format!(
                "Invalid longitude range: min_lon ({}) must be less than max_lon ({})",
                self.min_lon, self.max_lon
            ));
        }

        // Validate optional radius parameter
        if let Some(radius) = self.radius_meters {
            if radius < 100 {
                return Err(format!(
                    "Radius must be at least 100 meters, got: {}",
                    radius
                ));
            }

            if radius > 100000 {
                return Err(format!(
                    "Radius must be at most 100000 meters, got: {}",
                    radius
                ));
            }
        }

        Ok(())
    }

    /// Get the width of the bounding box in meters (simplified calculation)
    pub fn width_meters(&self) -> Option<f64> {
        // Convert degrees to approximate meters (1 degree ≈ 111,320 meters at equator)
        let lat_rad = self.min_lat.to_radians();
        let deg_to_m = 111320.0 * lat_rad.cos();

        Some((self.max_lon - self.min_lon) * deg_to_m)
    }

    /// Get the height of the bounding box in meters
    pub fn height_meters(&self) -> f64 {
        // Convert degrees to approximate meters (1 degree ≈ 111,320 meters)
        let deg_to_m = 111320.0;

        (self.max_lat - self.min_lat) * deg_to_m
    }
}

/// Create a point geometry from coordinates (for PostGIS)
pub fn make_point(lon: f64, lat: f64) -> String {
    format!("ST_MakePoint({lon}, {lat})::geography", lon = lon, lat = lat)
}

/// Calculate distance between two points using Haversine formula (for validation)
pub fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6371.0; // Earth radius in kilometers

    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin() * (dlat / 2.0).sin()
        + lat1.to_radians().cos() * lat2.to_radians().cos()
        * (dlon / 2.0).sin() * (dlon / 2.0).sin();
    let c = 2.0 * a.sqrt().atan2();
    R * c
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_radius_search_valid() {
        let query = RadiusSearchQuery {
            latitude: 40.7829,
            longitude: -73.9654,
            radius_meters: 1000,
        };
        assert!(query.validate().is_ok());
    }

    #[test]
    fn test_radius_search_invalid_latitude() {
        let query = RadiusSearchQuery {
            latitude: 95.0, // Invalid
            longitude: -73.9654,
            radius_meters: 1000,
        };
        assert!(query.validate().is_err());
    }

    #[test]
    fn test_radius_search_invalid_radius() {
        let query = RadiusSearchQuery {
            latitude: 40.7829,
            longitude: -73.9654,
            radius_meters: 50, // Too small
        };
        assert!(query.validate().is_err());
    }

    #[test]
    fn test_bounding_box_valid() {
        let bbox = BoundingBoxQuery {
            min_lat: 40.0,
            max_lat: 41.0,
            min_lon: -74.0,
            max_lon: -73.0,
            radius_meters: Some(5000),
        };
        assert!(bbox.validate().is_ok());
    }

    #[test]
    fn test_bounding_box_invalid_latitude_range() {
        let bbox = BoundingBoxQuery {
            min_lat: 41.0, // min > max
            max_lat: 40.0,
            min_lon: -74.0,
            max_lon: -73.0,
            radius_meters: Some(5000),
        };
        assert!(bbox.validate().is_err());
    }

    #[test]
    fn test_haversine_distance() {
        // New York to Boston ~300 km
        let distance = haversine_distance(40.7829, -73.9654, 42.3601, -71.0589);
        assert!((distance - 300.0).abs() < 10.0);
    }
}
