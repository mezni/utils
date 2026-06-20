mod config;
mod db;
mod error;
mod models;
mod routes;

use actix_web::{web, App, HttpServer};
use tracing_subscriber::EnvFilter;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cfg = config::Config::from_env();
    let pool = db::init_pool(&cfg.database_url)
        .await
        .expect("Failed to create database pool");

    db::run_migrations(&pool).await;

    tracing::info!(
        "Starting admin-service on {}:{}",
        cfg.host,
        cfg.port
    );

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .service(routes::health::health_check)
            .service(routes::partners::create)
            .service(routes::partners::list)
            .service(routes::partners::get_by_id)
            .service(routes::partners::update)
            .service(routes::partners::delete)
            .service(routes::stations::create)
            .service(routes::stations::list)
            .service(routes::stations::get_by_id)
            .service(routes::stations::update)
            .service(routes::stations::delete)
            .service(routes::chargers::create)
            .service(routes::chargers::list)
            .service(routes::chargers::get_by_id)
            .service(routes::chargers::update)
            .service(routes::chargers::delete)
    })
    .bind(format!("{}:{}", cfg.host, cfg.port))?
    .run()
    .await
}
