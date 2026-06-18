use serde::Serialize;
use sqlx::FromRow;

#[derive(Debug, Serialize, FromRow)]
pub struct NearbyStation {
    pub station_id: String,
    pub station_name: String,
    pub latitude: f64,
    pub longitude: f64,
    pub distance_meters: f64,
    pub is_private: bool,
    pub partner_name: Option<String>,
}
