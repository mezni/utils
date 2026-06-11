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
