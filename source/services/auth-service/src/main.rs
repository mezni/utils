mod config;
mod db;
mod error;
mod keycloak;
mod middleware;
mod models;
mod routes;
mod validation;

#[cfg(test)]
mod tests {
    use sqlx::PgPool;

    #[actix_web::test]
    async fn init_database(pool: PgPool) -> PgPool {
        pool
    }
}

use actix_cors::Cors;
use actix_web::{web, App, HttpServer};
use middleware::log_redaction;
use middleware::rate_limit::RateLimitMiddleware;
use config::Config;







#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Load environment variables
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_max_level(tracing::Level::INFO)
        .init();

    tracing::info!("Starting Auth Service...");

    let config = Config::new();

    tracing::info!("Connecting to database: {}", config.database_url);
    tracing::info!("Connecting to Keycloak: {}", config.keycloak_url);
    tracing::info!("Keycloak client ID: {}", config.keycloak_client_id);

    // Create database pool with connection limits
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(10)
        .max_lifetime(std::time::Duration::from_secs(300))
        .connect(&config.database_url)
        .await
        .expect("Failed to connect to database");

    // Create Keycloak client
    let keycloak_client = keycloak::KeycloakClient::new(config.keycloak_url.clone(), config.keycloak_client_id.clone());

    // Test database connection
    let _ = sqlx::query("SELECT 1")
        .fetch_one(&pool)
        .await
        .expect("Failed to test database connection");

    tracing::info!("Database connection successful");

    tracing::info!("Starting HTTP server on port {}", config.port);

    HttpServer::new(move || {
        App::new()
            .app_data(web::JsonConfig::default().limit(4 * 1024 * 1024)) // 4MB limit
            .app_data(web::Data::new(pool.clone())) // Clone pool for each worker
            .app_data(web::Data::new(keycloak_client.clone())) // Clone client for each worker
            .app_data(web::Data::new(config.clone())) // Clone config for each worker
            .wrap(Cors::permissive())
            .wrap(RateLimitMiddleware::default())
            .wrap(log_redaction::LogRedactionMiddleware::new())
            .configure(routes::configure)
    })
    .client_timeout(std::time::Duration::from_secs(30))
    .client_keep_alive(std::time::Duration::from_secs(75))
    .bind(("0.0.0.0", config.port))?
    .run()
    .await
}
