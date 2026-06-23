//! Main routes configuration

use actix_web::{web, Scope};

use crate::api::health;

/// Configure all routes
pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    // API routes
    cfg.service(web::scope("/api/v1").configure(|cfg| {
        web::scope("/analytics").configure(|cfg| {
            cfg.service(analytics::configure_routes);
        });
    }));

    // Health check routes
    cfg.service(web::resource("/health").route(web::get().to(health::health_check)));
    cfg.service(web::resource("/ready").route(web::get().to(health::ready_check)));
    cfg.service(web::resource("/live").route(web::get().to(health::live_check)));
}