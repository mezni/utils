use actix_web::web;
use crate::application::dashboard_service::DashboardService;
use bornemap_platform_core::result::ApiResponse;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct QueryParams {
    #[allow(dead_code)]
    _dummy: String,
}

#[derive(Serialize)]
pub struct KpiData {
    partners_count: i64,
    stations_count: i64,
    chargers_count: i64,
}

pub async fn get_kpis(
    _query: web::Query<QueryParams>,
    service: web::Data<DashboardService>,
) -> HttpResponse {
    match service.get_kpis().await {
        Ok(data) => HttpResponse::Ok().json(ApiResponse::ok(data)),
        Err(e) => {
            let resp = to_error_response(&e);
            HttpResponse::InternalServerError().json(resp)
        }
    }
}

pub async fn health() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "ok",
        "service": "admin-service"
    }))
}
