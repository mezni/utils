mod config;
mod db;
mod handlers;
mod models;
mod routes;

use actix_web::{App, HttpServer};
use tracing_subscriber::EnvFilter;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,driver_service=debug")),
        )
        .init();

    let cfg = config::AppConfig::from_env();
    let pool = db::init_pool(&cfg.database_url).await;

    tracing::info!(
        "Starting driver-service on {}:{}",
        cfg.host,
        cfg.port
    );

    HttpServer::new(move || {
        App::new()
            .app_data(actix_web::web::Data::new(pool.clone()))
            .configure(routes::configure)
    })
    .bind(format!("{}:{}", cfg.host, cfg.port))?
    .run()
    .await
}
