pub fn validate_station_id(id: &str) -> Result<(), String> {
    if id.is_empty() {
        return Err("station id cannot be empty".into());
    }
    if id.len() > 32 {
        return Err("station id too long (max 32)".into());
    }
    Ok(())
}

pub fn validate_latitude(lat: f64) -> Result<(), String> {
    if !(-90.0..=90.0).contains(&lat) {
        return Err("latitude must be between -90 and 90".into());
    }
    Ok(())
}

pub fn validate_longitude(lon: f64) -> Result<(), String> {
    if !(-180.0..=180.0).contains(&lon) {
        return Err("longitude must be between -180 and 180".into());
    }
    Ok(())
}
