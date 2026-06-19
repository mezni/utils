use actix_web::{web, App, HttpServer, middleware};
use sqlx::PgPool;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod error;
mod routes;
mod services;
mod middleware;

use error::AuthError;

#[actix_web::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    let env_filter = LevelFilter::from_env("RUST_LOG")
        .unwrap_or_else(|| LevelFilter::INFO);

    let subscriber = tracing_subscriber::Registry::default()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer());

    subscriber.init();

    tracing::info!("Starting Admin Service on port 3002");

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL must be set");

    // Create database pool
    let pool = PgPool::connect(&db_url).await?;
    tracing::info!("Connected to PostgreSQL database");

    // Initialize Redis client
    let redis_client = redis::Client::open(redis_url)?;
    let redis_conn = redis_client.get_async_connection().await?;
    tracing::info!("Connected to Redis");

    // Create application
    let pool = web::Data::new(pool);
    let redis_conn = web::Data::new(redis_conn);

    // Build HTTP server
    HttpServer::new(move || {
        App::new()
            .app_data(pool.clone())
            .app_data(redis_conn.clone())
            .wrap(middleware::Logger::default())
            .wrap(middleware::Compress::default())
            .configure(routes::config)
    })
    .bind(("0.0.0.0", 3002))?
    .run()
    .await?;

    Ok(())
}
