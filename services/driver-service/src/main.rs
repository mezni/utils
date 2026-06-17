mod config;
mod db;
mod handler;
mod middleware;
mod models;
mod repository;
mod routes;

use actix_web::web::Data;
use actix_web::{App, HttpServer, middleware as actix_middleware};
use config::Config;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let cfg = Config::from_env();

    std::env::set_var("RUST_LOG", &cfg.log_level);
    env_logger::init();

    let database_url = cfg.database_url.clone();

    log::info!("Connecting to database...");
    let pool = db::init_pool(&database_url).await;

    log::info!("Starting driver-service on {}:{}", cfg.host, cfg.port);

    let pool_data = Data::new(pool);

    HttpServer::new(move || {
        App::new()
            .wrap(actix_middleware::Logger::default())
            .configure(|cfg| routes::setup_routes(cfg, pool_data.clone()))
    })
    .bind(format!("{}:{}", cfg.host, cfg.port))?
    .run()
    .await
}
