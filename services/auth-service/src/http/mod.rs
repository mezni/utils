pub mod auth;
pub mod dto;
pub mod error;
pub mod health;
pub mod oauth;

use actix_web::web;

pub fn configure(cfg: &mut web::ServiceConfig, oauth_state: oauth::OAuthState) {
    cfg.service(health::live)
        .service(health::ready)
        .service(auth::register)
        .service(auth::login)
        .service(auth::refresh)
        .service(auth::logout);
    
    // Configure OAuth routes
    oauth::configure_oauth_routes(cfg, oauth_state);
}
