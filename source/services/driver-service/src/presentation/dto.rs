use serde::Serialize;
use crate::domain::station::Station;

#[derive(Serialize)]
pub struct NearbyStationResponse {
    pub station_id: String,
    pub name: Option<String>,
    pub lat: f64,
    pub lon: f64,
    pub distance_km: f64,
}

impl From<Station> for NearbyStationResponse {
    fn from(s: Station) -> Self {
        Self {
            station_id: s.station_id,
            name: s.name,
            lat: s.lat,
            lon: s.lon,
            distance_km: s.distance_km,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dto_from_station() {
        let station = Station::new(
            "STA-abc123".into(),
            Some("Station".into()),
            36.8, 10.1, 1.5,
        );
        let dto: NearbyStationResponse = station.into();
        assert_eq!(dto.station_id, "STA-abc123");
        assert_eq!(dto.name, Some("Station".into()));
        assert_eq!(dto.distance_km, 1.5);
    }

    #[test]
    fn test_dto_json_shape() {
        let station = Station::new(
            "STA-test".into(),
            None,
            36.0, 10.0, 0.5,
        );
        let dto: NearbyStationResponse = station.into();
        let json = serde_json::to_value(&dto).unwrap();
        assert!(json.get("station_id").is_some());
        assert!(json.get("distance_km").is_some());
        assert!(json.get("lat").is_some());
        assert!(json.get("lon").is_some());
    }
}
