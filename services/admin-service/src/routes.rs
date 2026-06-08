use actix_web::web;

pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg
        .route("/api/v1/health", web::get().to(super::handlers::health_check_handler))
        .route("/api/v1/partners", web::post().to(super::handlers::partner_create_handler))
        .route("/api/v1/partners", web::get().to(super::handlers::partner_list_handler))
        .route("/api/v1/partners/{id}", web::get().to(super::handlers::partner_get_handler))
        .route("/api/v1/partners/{id}", web::put().to(super::handlers::partner_update_handler))
        .route("/api/v1/partners/{id}", web::delete().to(super::handlers::partner_delete_handler))
        .route("/api/v1/stations", web::post().to(super::handlers::station_create_handler))
        .route("/api/v1/stations", web::get().to(super::handlers::station_list_handler))
        .route("/api/v1/stations/{id}", web::get().to(super::handlers::station_get_handler))
        .route("/api/v1/stations/{id}", web::put().to(super::handlers::station_update_handler))
        .route("/api/v1/stations/{id}", web::delete().to(super::handlers::station_delete_handler))
        .route("/api/v1/chargers", web::post().to(super::handlers::charger_create_handler))
        .route("/api/v1/chargers", web::get().to(super::handlers::charger_list_handler))
        .route("/api/v1/chargers/{id}", web::get().to(super::handlers::charger_get_handler))
        .route("/api/v1/chargers/{id}", web::put().to(super::handlers::charger_update_handler))
        .route("/api/v1/chargers/{id}", web::delete().to(super::handlers::charger_delete_handler));
}
