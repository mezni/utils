use actix_web::web;

use crate::handlers::health_handler;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.route("", web::get().to(health_handler::health_check));
}
