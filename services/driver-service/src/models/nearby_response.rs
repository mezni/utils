use serde::{Deserialize, Serialize};
use crate::models::station::Station;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NearbyResponse {
    pub stations: Vec<Station>,
    pub count: i32,
    pub radius_m: i32,
}
