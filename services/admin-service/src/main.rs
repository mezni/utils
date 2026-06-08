mod config;
mod db;
mod error;
mod handlers;
mod models;
mod routes;

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use actix_web::{middleware::Logger, web, App, HttpServer};

use config::PostgresUrl;
use db::create_pool;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    dotenv::dotenv().ok();

    let postgres_url = PostgresUrl::new(
        std::env::var("POSTGRES_URL")
            .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432/ev_platform".to_string()),
    );

    if let Err(e) = postgres_url.validate() {
        tracing::error!("Invalid POSTGRES_URL: {}", e);
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            e,
        ));
    }

    tracing::info!("Initializing admin service with database URL: {}", postgres_url.url);

    let pool = create_pool(&postgres_url)
        .await
        .expect("Failed to create database connection pool");

    tracing::info!("Database connection pool created successfully");

    tracing::info!("Applying database migrations...");
    if let Err(e) = db::apply_migrations(&pool).await {
        tracing::error!("Failed to apply migrations: {}", e);
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            e,
        ));
    }

    tracing::info!("Database migrations applied successfully");

    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse::<u16>()
        .expect("PORT must be a valid number");

    tracing::info!("Starting admin service on port {}", port);

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .configure(routes::configure_routes)
            .app_data(web::Data::new(pool.clone()))
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await
}
