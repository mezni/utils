use actix_web::web;
use crate::handlers;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v1/public")
            .route("/stations", web::get().to(handlers::get_stations))
            .route("/config", web::get().to(handlers::get_config))
            .route("/telemetry", web::post().to(handlers::ingest_telemetry)),
    );
}

pub mod admin {
    use actix_web::web;

    pub fn configure(cfg: &mut web::ServiceConfig) {
        cfg.service(
            web::scope("/api/v1/admin")
                .route("/partners", web::get().to(|| async { "TODO" }))
                .route("/stations", web::post().to(|| async { "TODO" })),
        );
    }
}
