use actix_web::{web, HttpResponse};
use sqlx::PgPool;
use std::time::Duration;

pub async fn health(pool: web::Data<PgPool>) -> HttpResponse {
    let healthy = tokio::time::timeout(Duration::from_millis(500), pool.acquire())
        .await
        .ok()
        .and_then(|r| r.ok())
        .is_some();

    if healthy {
        HttpResponse::Ok().json(serde_json::json!({"status": "ok"}))
    } else {
        HttpResponse::ServiceUnavailable().json(serde_json::json!({"status": "degraded"}))
    }
}
