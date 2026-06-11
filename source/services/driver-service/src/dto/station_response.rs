use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct StationResponse {
    pub id: String,
    pub name: String,
    pub address: Option<String>,
    pub latitude: f64,
    pub longitude: f64,
}

impl From<borne_data::Station> for StationResponse {
    fn from(s: borne_data::Station) -> Self {
        StationResponse {
            id: s.id,
            name: s.name,
            address: s.address,
            latitude: s.latitude,
            longitude: s.longitude,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_correctly() {
        let s = StationResponse {
            id: "st_1".into(),
            name: "Station 1".into(),
            address: Some("123 Main St".into()),
            latitude: 36.8,
            longitude: 10.18,
        };
        let json = serde_json::to_value(&s).unwrap();
        assert_eq!(json["id"], "st_1");
        assert_eq!(json["name"], "Station 1");
        assert_eq!(json["address"], "123 Main St");
        assert_eq!(json["latitude"], 36.8);
        assert_eq!(json["longitude"], 10.18);
    }

    #[test]
    fn serializes_null_address() {
        let s = StationResponse {
            id: "st_2".into(),
            name: "No Address".into(),
            address: None,
            latitude: 0.0,
            longitude: 0.0,
        };
        let json = serde_json::to_value(&s).unwrap();
        assert!(json["address"].is_null());
    }

    #[test]
    fn from_station() {
        let station = borne_data::Station {
            id: "s1".into(),
            partner_id: "p1".into(),
            name: "Test".into(),
            address: Some("Addr".into()),
            latitude: 1.0,
            longitude: 2.0,
            created_at: borne_data::chrono::Utc::now(),
            created_by: None,
            updated_at: borne_data::chrono::Utc::now(),
            updated_by: None,
        };
        let resp: StationResponse = station.into();
        assert_eq!(resp.id, "s1");
        assert_eq!(resp.name, "Test");
        assert_eq!(resp.address, Some("Addr".into()));
    }
}
