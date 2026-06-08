// Routes module
use actix_web::web;

pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg
        // Health check endpoint
        .route("/api/v1/health", web::get().to(health_check))
        // Partner CRUD endpoints
        .route("/api/v1/partners", web::post().to(partner_create_handler))
        .route("/api/v1/partners", web::get().to(partner_list_handler))
        .route("/api/v1/partners/{id}", web::get().to(partner_get_handler))
        .route("/api/v1/partners/{id}", web::put().to(partner_update_handler))
        .route("/api/v1/partners/{id}", web::delete().to(partner_delete_handler))
        // Station CRUD endpoints
        .route("/api/v1/stations", web::post().to(station_create_handler))
        .route("/api/v1/stations", web::get().to(station_list_handler))
        .route("/api/v1/stations/{id}", web::get().to(station_get_handler))
        .route("/api/v1/stations/{id}", web::put().to(station_update_handler))
        .route("/api/v1/stations/{id}", web::delete().to(station_delete_handler))
        // Charger CRUD endpoints
        .route("/api/v1/chargers", web::post().to(charger_create_handler))
        .route("/api/v1/chargers", web::get().to(charger_list_handler))
        .route("/api/v1/chargers/{id}", web::get().to(charger_get_handler))
        .route("/api/v1/chargers/{id}", web::put().to(charger_update_handler))
        .route("/api/v1/chargers/{id}", web::delete().to(charger_delete_handler));
}

pub async fn health_check() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "ok",
        "service": "admin-service",
        "db": "ok"
    }))
}
