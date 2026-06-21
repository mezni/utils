#[derive(Debug, Clone, serde::Serialize)]
pub struct NearbyResult {
    pub station_id: String,
    pub name: String,
    pub distance_meters: f64,
}

impl NearbyResult {
    pub fn new(station_id: String, name: String, distance_meters: f64) -> Self {
        Self { station_id, name, distance_meters }
    }
}
