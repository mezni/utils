use actix_web::{get, HttpResponse};
use serde::Serialize;

#[derive(Serialize)]
struct HealthResponse {
    status: String,
    service: String,
    version: String,
}

#[get("/api/v1/health")]
pub async fn health() -> HttpResponse {
    HttpResponse::Ok().json(HealthResponse {
        status: "ok".into(),
        service: "gis-service".into(),
        version: "0.1.0".into(),
    })
}
