use actix_web::{web, HttpResponse, Result};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct CreateStationRequest {
    pub partner_id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    pub location: GeoLocation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub osm_id: Option<i64>,
}

#[derive(Deserialize)]
pub struct GeoLocation {
    #[serde(rename = "type")]
    pub location_type: String,
    pub coordinates: Vec<f64>,
}

pub async fn create_station(
    _pool: web::Data<sqlx::PgPool>,
    _req: web::Json<CreateStationRequest>,
) -> Result<HttpResponse> {
    // TODO: Implement T035 - Implement create_station endpoint (POST /api/v1/admin/station) with transaction, audit, MV refresh, and cache bust
    Ok(HttpResponse::NotImplemented().json(serde_json::json!({
        "error": "Not implemented",
        "message": "T035: Implement create_station endpoint (POST /api/v1/admin/station) with transaction, audit, MV refresh, and cache bust"
    })))
}
