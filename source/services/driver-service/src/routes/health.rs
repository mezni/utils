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
        .execute(&state.db_pool)
        .await;

    let (status, db) = match db_status {
        Ok(_) => ("ok".into(), "connected".into()),
        Err(_) => ("degraded".into(), "disconnected".into()),
    };

    HttpResponse::Ok().json(HealthResponse {
        status,
        service: state.service_name.clone(),
        database: db,
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_secs: state.startup_time.elapsed().as_secs(),
    })
}
