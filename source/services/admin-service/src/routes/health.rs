use actix_web::get;
use actix_web::HttpResponse;

#[get("/api/health")]
pub async fn health() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION")
    }))
}
