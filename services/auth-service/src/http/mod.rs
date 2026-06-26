pub mod auth;
pub mod dto;
pub mod error;
pub mod health;

use actix_web::web;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(health::live)
        .service(health::ready)
        .service(auth::register)
        .service(auth::login)
        .service(auth::refresh)
        .service(auth::logout);
}
