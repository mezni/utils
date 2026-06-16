use actix_web::{get, web, HttpResponse};
use sqlx::PgPool;

#[get("/api/v1/health/ready")]
pub async fn ready(pool: web::Data<PgPool>) -> HttpResponse {
    match sqlx::query("SELECT 1").execute(pool.get_ref()).await {
        Ok(_) => HttpResponse::Ok().json(serde_json::json!({
            "status": "ready",
            "service": "driver-service",
        })),
        Err(_) => HttpResponse::ServiceUnavailable().json(serde_json::json!({
            "status": "not ready",
            "service": "driver-service",
        })),
    }
}
