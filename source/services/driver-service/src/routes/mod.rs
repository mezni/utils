use actix_web::web;
use crate::handlers;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v1")
            .route("/health", web::get().to(handlers::health))
            .route("/stations/nearby", web::get().to(handlers::get_nearby_stations)),
    );
}
