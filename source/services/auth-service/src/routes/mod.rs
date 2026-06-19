use actix_web::{web, HttpResponse, Result};

pub mod login;
pub mod refresh;
pub mod logout;
pub mod me;

pub fn configure(cfg: &mut web::ServiceConfig) {
    // Login endpoint
    cfg.route("/login", web::post().to(login));

    // Refresh endpoint
    cfg.route("/refresh", web::post().to(refresh));

    // Logout endpoint
    cfg.route("/logout", web::post().to(logout));

    // Profile endpoint
    cfg.route("/me", web::get().to(me));

    // Health check endpoint
    cfg.route("/health", web::get().to(health_check));
}

async fn health_check() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "healthy",
        "service": "auth-service"
    }))
}
