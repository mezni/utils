use actix_web::{get, web, HttpResponse, Responder};
use serde::Serialize;

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    db: &'static str,
    timestamp: String,
}

#[get("/api/v1/driver/health")]
pub async fn health_check(pool: web::Data<sqlx::PgPool>) -> impl Responder {
    match sqlx::query("SELECT 1").execute(pool.get_ref()).await {
        Ok(_) => HttpResponse::Ok().json(HealthResponse {
            status: "ok",
            db: "connected",
            timestamp: chrono::Utc::now().to_rfc3339(),
        }),
        Err(_) => HttpResponse::ServiceUnavailable().json(HealthResponse {
            status: "error",
            db: "disconnected",
            timestamp: chrono::Utc::now().to_rfc3339(),
        }),
    }
}
