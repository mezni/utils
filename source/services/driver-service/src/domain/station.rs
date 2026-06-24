use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct Station {
    pub station_id: String,
    pub name: Option<String>,
    pub lat: f64,
    pub lon: f64,
    pub distance_km: f64,
}

impl Station {
    pub fn new(station_id: String, name: Option<String>, lat: f64, lon: f64, distance_km: f64) -> Self {
        Self { station_id, name, lat, lon, distance_km }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_station_construction() {
        let s = Station::new(
            "STA-test123".into(),
            Some("Test Station".into()),
            36.8, 10.1, 1.23,
        );
        assert_eq!(s.station_id, "STA-test123");
        assert_eq!(s.name, Some("Test Station".into()));
        assert_eq!(s.lat, 36.8);
        assert_eq!(s.lon, 10.1);
        assert_eq!(s.distance_km, 1.23);
    }

    #[test]
    fn test_station_null_name() {
        let s = Station::new(
            "STA-nullname".into(),
            None,
            36.0, 10.0, 5.0,
        );
        assert_eq!(s.name, None);
    }

    #[test]
    fn test_station_serialization() {
        let s = Station::new(
            "STA-serialize".into(),
            Some("S".into()),
            36.0, 10.0, 2.0,
        );
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains("station_id"));
        assert!(json.contains("distance_km"));
    }
}
