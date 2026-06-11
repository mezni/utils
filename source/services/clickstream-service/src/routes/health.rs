use actix_web::{web, HttpResponse};

use crate::db::repository::AnalyticsDbRepo;
use crate::response::ApiResponse;

pub async fn health_check(repo: web::Data<AnalyticsDbRepo>) -> HttpResponse {
    match repo.health_check().await {
        Ok(true) => HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
            "status": "ok",
            "database": "connected",
        }))),
        _ => HttpResponse::ServiceUnavailable().json(ApiResponse::success(serde_json::json!({
            "status": "degraded",
            "database": "disconnected",
        }))),
    }
}
