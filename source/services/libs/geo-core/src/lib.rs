pub const TUNISIA_MIN_LON: f64 = 7.0000;
pub const TUNISIA_MAX_LON: f64 = 12.0000;
pub const TUNISIA_MIN_LAT: f64 = 30.0000;
pub const TUNISIA_MAX_LAT: f64 = 38.0000;

pub fn is_within_tunisia(lon: f64, lat: f64) -> bool {
    lon >= TUNISIA_MIN_LON
        && lon <= TUNISIA_MAX_LON
        && lat >= TUNISIA_MIN_LAT
        && lat <= TUNISIA_MAX_LAT
}
