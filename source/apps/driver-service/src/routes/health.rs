use actix_web::{get, HttpResponse};
use crate::models::HealthResponse;

#[get("/api/health")]
pub async fn health() -> HttpResponse {
    HttpResponse::Ok().json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}
