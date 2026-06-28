use actix_cors::Cors;
use actix_web::{web, App, HttpServer, middleware};

mod config;
mod presentation;

mod application;
mod domain;
mod infrastructure;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();
    env_logger::init();

    let cfg = common_config::load_config("auth").expect("Failed to load config");

    log::info!(
        "Starting auth-service on {}:{}",
        cfg.host,
        cfg.port
    );

    let pool = common_db::pool::create_pool(
        &cfg.database.url(),
        cfg.max_db_connections.unwrap_or(10),
    )
    .await
    .expect("Failed to create database pool");

    let pool_data = web::Data::new(pool);

    HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header()
            .max_age(3600);

        App::new()
            .wrap(cors)
            .wrap(middleware::Logger::default())
            .app_data(pool_data.clone())
            .configure(config::routes::configure)
    })
    .bind(format!("{}:{}", cfg.host, cfg.port))?
    .run()
    .await
}
