use actix_web::{web, HttpResponse, Result};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct CreateChargerRequest {
    pub station_id: String,
    pub connector_type_id: i32,
    pub status_id: i32,
    pub current_type_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub power_kw: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub voltage: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amperage: Option<i32>,
    pub count_available: i32,
    pub count_total: i32,
}

pub async fn create_charger(
    _pool: web::Data<sqlx::PgPool>,
    _req: web::Json<CreateChargerRequest>,
) -> Result<HttpResponse> {
    // TODO: Implement T046 - Implement create_charger endpoint (POST /api/v1/admin/charger) with transaction, audit, MV refresh, and cache bust
    Ok(HttpResponse::NotImplemented().json(serde_json::json!({
        "error": "Not implemented",
        "message": "T046: Implement create_charger endpoint (POST /api/v1/admin/charger) with transaction, audit, MV refresh, and cache bust"
    })))
}
