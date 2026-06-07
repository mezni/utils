// Routes module
use actix_web::web;

pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg
        // Health check endpoint
        .route("/api/v1/health", web::get().to(health_check))
        // Stations nearby endpoint
        .route("/api/v1/stations/nearby", web::get().to(stations_nearby));
}

pub async fn health_check() -> HttpResponse {
    // Health check handler is defined in handlers.rs
    HttpResponse::Ok().json(serde_json::json!({
        "status": "ok",
        "service": "driver-service",
        "db": "ok"
    }))
}

pub async fn stations_nearby() -> HttpResponse {
    // Stations nearby handler is defined in handlers.rs
    HttpResponse::Ok().json(serde_json::json!({
        "stations": []
    }))
}
