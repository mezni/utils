use actix_web::{web, HttpResponse};
use serde::Serialize;
use crate::AppState;

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub service: String,
    pub database: String,
    pub version: String,
    pub uptime_secs: u64,
}

pub async fn health_check(state: web::Data<AppState>) -> HttpResponse {
    let db_status = sqlx::query("SELECT 1")
        .execute(&state.platform_db)
        .await;

    let analytics_status = sqlx::query("SELECT 1")
        .execute(&state.analytics_db)
        .await;

    let db = match (db_status, analytics_status) {
        (Ok(_), Ok(_)) => "connected",
        (Ok(_), Err(_)) => "degraded (analytics_db down)",
        (Err(_), Ok(_)) => "degraded (platform_db down)",
        (Err(_), Err(_)) => "disconnected",
    };
    let status = if db == "disconnected" { "down" } else { "ok" };

    HttpResponse::Ok().json(HealthResponse {
        status: status.into(),
        service: state.service_name.clone(),
        database: db.into(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_secs: state.startup_time.elapsed().as_secs(),
    })
}
