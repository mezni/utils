use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreatePartnerRequest {
    pub name: String,
    pub partner_type: String,
    pub email: String,
    pub phone: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateStationRequest {
    pub partner_id: String,
    pub name: String,
    pub address: String,
    pub email: String,
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UpdateStationLiveRequest {
    pub is_live: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateChargerRequest {
    pub station_id: String,
    pub identifier_code: String,
    pub plug_type_code: String,
    pub max_power_kw: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateResponse<T> {
    pub data: T,
    pub message: String,
}
