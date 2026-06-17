mod config;
mod db;
mod routes;
mod handler;
mod middleware;
mod repository;
mod models;

use actix_web::web::Data;
use actix_web::{App, HttpServer, middleware};
use config::Config;
use sqlx::PgPool;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let cfg = Config::from_env();

    std::env::set_var("RUST_LOG", &cfg.log_level);
    env_logger::init();

    let database_url = cfg.database_url.clone();

    // Connect to database
    let pool = PgPool::connect(&database_url).await?;

    log::info!("Starting driver-service on {}:{}", cfg.host, cfg.port);

    HttpServer::new(move || {
        let pool_data = web::Data::new(pool.clone());
        App::new()
            .app_data(pool_data)
            .wrap(middleware::Logger::default())
            .configure(|config| routes::setup_routes(config, pool_data))
    })
    .bind(format!("{}:{}", cfg.host, cfg.port))?
    .run()
    .await
}
