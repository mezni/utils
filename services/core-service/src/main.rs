use actix_web::{web, App, HttpServer, middleware::Logger};
use std::env;

mod config;
mod middleware;
mod utils;
mod handlers;
mod services;
mod repositories;
mod models;

use middleware::{jwt_auth};
use handlers::{health_handler, company_handler, station_handler, charger_handler};
use services::{CompanyService, StationService, ChargerService};
use utils::database::Database;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize logging
    env_logger::init();

    // Load configuration
    let config = config::Config::from_env().expect("Failed to load configuration");
    
    let server_port = config.server.port;

    log::info!("Starting core service on port {}...", server_port);

    // Create database connection pool
    let database = Database::new(&config.database.url)
        .await
        .expect("Failed to create database pool");

    log::info!("Database connection pool initialized");

    // Create services
    let company_service = std::sync::Arc::new(CompanyService::new(database.clone()));
    let station_service = std::sync::Arc::new(StationService::new(database.clone(), company_service.clone()));
    let charger_service = std::sync::Arc::new(ChargerService::new(database.clone(), station_service.clone()));

    log::info!("Services initialized");

    // Set application start time for health checks
    handlers::health_handler::set_app_start_time();

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(database.clone()))
            .app_data(web::Data::new(company_service.clone()))
            .app_data(web::Data::new(station_service.clone()))
            .app_data(web::Data::new(charger_service.clone()))
            .wrap(Logger::default())
            .wrap(jwt_auth())
            .route("/health/core-service", web::get().to(health_handler::health_check))
            .route("/metrics/core-service", web::get().to(health_handler::metrics))
            .service(
                web::scope("/api/v1")
                    .configure(configure_routes)
                    .configure(company_handler::configure_company_routes)
                    .configure(station_handler::configure_station_routes)
                    .configure(charger_handler::configure_charger_routes)
            )
    })
    .bind(format!("0.0.0.0:{}", server_port))?
    .run()
    .await
}

fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::get().to(|| async { "Core Service API v1" }))
    );
}