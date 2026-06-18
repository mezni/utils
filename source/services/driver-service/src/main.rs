use actix_cors::Cors;
use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};

use driver_service::api;
use driver_service::config::Config;
use driver_service::db;
use driver_service::logging;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();
    logging::init();

    let config = Config::from_env();
    let pool = db::pool::create_pool(
        &config.database_url,
        config.db_pool_min,
        config.db_pool_max,
    )
    .await
    .expect("Failed to create database pool");

    tracing::info!(
        bind_addr = %config.listen_addr,
        pool_size = config.db_pool_max,
        "Starting driver-service"
    );

    HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header()
            .max_age(3600);

        App::new()
            .wrap(cors)
            .wrap(Logger::default())
            .app_data(web::Data::new(pool.clone()))
            .configure(api::configure)
    })
    .bind(&config.listen_addr)?
    .run()
    .await
}
