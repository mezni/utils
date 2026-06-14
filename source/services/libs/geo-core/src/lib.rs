pub const TUNISIA_MIN_LON: f64 = 7.0000;
pub const TUNISIA_MAX_LON: f64 = 12.0000;
pub const TUNISIA_MIN_LAT: f64 = 30.0000;
pub const TUNISIA_MAX_LAT: f64 = 38.0000;

pub fn is_within_tunisia(lon: f64, lat: f64) -> bool {
    lon >= TUNISIA_MIN_LON && lon <= TUNISIA_MAX_LON &&
    lat >= TUNISIA_MIN_LAT && lat <= TUNISIA_MAX_LAT
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tunis_center_within_bounds() {
        assert!(is_within_tunisia(10.1815, 36.8065));
    }

    #[test]
    fn test_outside_bounds_rejects() {
        assert!(!is_within_tunisia(0.0, 0.0));
    }
}
