use actix_web::web;

pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg
        .route("/api/v1/health", web::get().to(super::handlers::health_check_handler))
        .route("/api/v1/stations/nearby", web::get().to(super::handlers::stations_nearby_handler));
}
