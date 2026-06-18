mod health;
mod nearby;

use actix_web::web;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v1")
            .route("/nearby", web::get().to(nearby::nearby)),
    )
    .route("/health", web::get().to(health::health));
}
