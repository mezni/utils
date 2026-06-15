use actix_web::web;
use crate::handlers;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.route("/api/v1/health", web::get().to(handlers::health));
    cfg.route("/api/v1/stations/nearby", web::get().to(handlers::get_nearby_stations));
    cfg.route("/health", web::get().to(handlers::health));
    cfg.route("/stations/nearby", web::get().to(handlers::get_nearby_stations));
}
