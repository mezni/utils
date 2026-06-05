use actix_web::web;
use crate::interface::handlers;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.configure(handlers::health::configure);
}
