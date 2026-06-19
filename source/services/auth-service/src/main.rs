mod config;
mod db;
mod error;
mod keycloak;
mod middleware;
mod models;
mod routes;
mod validation;

use actix_cors::Cors;
use actix_web::{web, App, HttpServer};
use middleware::log_redaction;
use middleware::rate_limit::RateLimitMiddleware;

/// Get the database URL from environment variables.
fn get_db_url() -> String {
    std::env::var("DATABASE_URL")
        .expect("DATABASE_URL environment variable must be set")
}

/// Get the Keycloak URL from environment variables.
fn get_keycloak_url() -> String {
    std::env::var("KEYCLOAK_URL")
        .expect("KEYCLOAK_URL environment variable must be set")
}

/// Get the Keycloak client ID from environment variables.
fn get_keycloak_client_id() -> String {
    std::env::var("KEYCLOAK_CLIENT_ID")
        .unwrap_or_else(|_| "auth-service".to_string())
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Load environment variables
    std::env::set_var("RUST_LOG", "auth_service=debug");
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    tracing::info!("Starting Auth Service...");

    // Get configuration
    let db_url = get_db_url();
    let keycloak_url = get_keycloak_url();
    let keycloak_client_id = get_keycloak_client_id();

    tracing::info!("Connecting to database: {}", db_url);
    tracing::info!("Connecting to Keycloak: {}", keycloak_url);
    tracing::info!("Keycloak client ID: {}", keycloak_client_id);

    // Create database pool with connection limits
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(10)
        .max_lifetime(std::time::Duration::from_secs(300))
        .connect(&db_url)
        .await
        .expect("Failed to connect to database");

    // Create Keycloak client
    let keycloak_client = keycloak::KeycloakClient::new(keycloak_url);

    // Test database connection
    let _ = sqlx::query("SELECT 1")
        .fetch_one(&pool)
        .await
        .expect("Failed to test database connection");

    tracing::info!("Database connection successful");

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3000);

    tracing::info!("Starting HTTP server on port {}", port);

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool))
            .app_data(web::Data::new(&keycloak_client))
            .wrap(Cors::permissive())
            .wrap(RateLimitMiddleware::default())
            .wrap(log_redaction::LogRedactionMiddleware::new())
            .configure(routes::configure)
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await
}
