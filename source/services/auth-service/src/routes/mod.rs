use actix_web::{web, HttpResponse, Result};

pub mod login;
pub mod refresh;
pub mod logout;
pub mod me;

pub use login::{login, configure as configure_login};
pub use refresh::{refresh, configure as configure_refresh};
pub use logout::{logout, configure as configure_logout};
pub use me::{me, configure as configure_me};

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::scope("/api/v1/auth").configure(configure_login));
    cfg.service(web::scope("/api/v1/auth").configure(configure_refresh));
    cfg.service(web::scope("/api/v1/auth").configure(configure_logout));
    cfg.service(web::scope("/api/v1/auth").configure(configure_me));
    cfg.route("/health", web::get().to(health_check));
}

async fn health_check() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "healthy",
        "service": "auth-service"
    }))
}
