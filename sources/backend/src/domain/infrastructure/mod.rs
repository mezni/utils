pub mod mod_impl;
pub mod repository;

pub use mod_impl::nearby_stations;

use serde::Serialize;
use sqlx::FromRow;

#[derive(Debug, Serialize, FromRow)]
pub struct NearbyStationResult {
    pub station_id: String,
    pub station_name: String,
    pub address: String,
    pub city: String,
    pub longitude: f64,
    pub latitude: f64,
    pub distance_meters: f64,
    pub available_chargers_count: i64,
    pub is_test: bool,
}
