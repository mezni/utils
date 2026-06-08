// Main module
mod config;
mod db;
mod error;
mod handlers;
mod models;
mod routes;

use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use actix_web::{middleware::Logger, web, App, HttpServer};

use config::PostgresUrl;
use db::create_pool;
use handlers::{health_check_handler, partner_create_handler, partner_get_handler, partner_list_handler, partner_update_handler, partner_delete_handler, station_create_handler, station_get_handler, station_list_handler, station_update_handler, station_delete_handler, charger_create_handler, charger_get_handler, charger_list_handler, charger_update_handler, charger_delete_handler};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Load environment variables from .env file
    dotenv::dotenv().ok();

    // Get database URL from environment
    let postgres_url = PostgresUrl::new(
        std::env::var("POSTGRES_URL")
            .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432/ev_platform".to_string()),
    );

    // Validate database URL
    if let Err(e) = postgres_url.validate() {
        tracing::error!("Invalid POSTGRES_URL: {}", e);
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            e,
        ));
    }

    tracing::info!("Initializing admin service with database URL: {}", postgres_url.url);

    // Create database connection pool
    let pool = create_pool(&postgres_url)
        .await
        .expect("Failed to create database connection pool");

    tracing::info!("Database connection pool created successfully");

    // Apply migrations on startup
    tracing::info!("Applying database migrations...");
    if let Err(e) = db::apply_migrations(&pool).await {
        tracing::error!("Failed to apply migrations: {}", e);
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            e,
        ));
    }

    tracing::info!("Database migrations applied successfully");

    // Create PostgresUrl as Arc for sharing with handlers
    let postgres_url_arc = Arc::new(postgres_url);

    // Start HTTP server
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse::<u16>()
        .expect("PORT must be a valid number");

    tracing::info!("Starting admin service on port {}", port);

    HttpServer::new(move || {
        App::new()
            // Global middleware
            .wrap(Logger::default())
            // Route configuration
            .configure(routes::configure_routes)
            // Dependency injection for handlers
            .app_data(web::Data::new(postgres_url_arc.clone()))
            .app_data(web::Data::new(pool.clone()))
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await
}
