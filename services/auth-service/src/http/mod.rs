pub mod admin_metrics;
pub mod auth;
pub mod dto;
pub mod error;
pub mod health;
pub mod metrics;
pub mod middleware;
pub mod oauth;
pub mod extractors;

use actix_web::web;

pub fn configure(cfg: &mut web::ServiceConfig, oauth_state: oauth::OAuthState) {
    // Admin routes - require authentication and ADMIN role
    cfg.service(
        web::scope("/api/v1/admin")
            .service(
                web::scope("")
                    .service(admin_metrics::users_metrics)
            )
    );

    // Public routes
    cfg.service(health::live)
        .service(health::ready)
        .service(metrics::metrics_handler)
        .service(admin_metrics::users_metrics)
        .service(auth::register)
        .service(auth::login)
        .service(auth::refresh)
        .service(auth::logout);

    oauth::configure_oauth_routes(cfg, oauth_state);
}
