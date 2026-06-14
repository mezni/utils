mod config;
mod error;

use std::net::SocketAddr;
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

use config::Config;
use error::DomainError;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let config = Config::from_env().expect("Failed to load configuration");

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&config.rust_log))
        .init();

    let pool = create_db_pool(&config).await;

    let pool = match pool {
        Ok(pool) => {
            tracing::info!("Database connected successfully");
            pool
        }
        Err(e) => {
            tracing::error!("Failed to connect to database: {}", e);
            std::process::exit(1);
        }
    };

    let app = driver_service::build_router(pool).await;

    let addr = SocketAddr::new(
        config.host.parse().expect("Invalid HOST address"),
        config.port,
    );

    tracing::info!("Starting server on {}", addr);

    let listener = TcpListener::bind(addr)
        .await
        .expect("Failed to bind address");

    axum::serve(listener, app)
        .await
        .expect("Server error");
}

async fn create_db_pool(config: &Config) -> Result<sqlx::PgPool, DomainError> {
    let mut last_error = None;

    for attempt in 1..=config.db_connect_retries {
        match PgPoolOptions::new()
            .max_connections(config.db_pool_size)
            .connect(&config.database_url)
            .await
        {
            Ok(pool) => {
                tracing::info!(
                    "Database connected on attempt {}/{}",
                    attempt,
                    config.db_connect_retries
                );
                return Ok(pool);
            }
            Err(e) => {
                tracing::warn!(
                    "DB connection attempt {}/{} failed: {}",
                    attempt,
                    config.db_connect_retries,
                    e
                );
                last_error = Some(e);
                if attempt < config.db_connect_retries {
                    let delay =
                        Duration::from_millis(config.db_retry_base_delay_ms * 2u64.pow(attempt - 1));
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    Err(DomainError::ServiceUnavailable(format!(
        "Failed to connect after {} retries: {:?}",
        config.db_connect_retries,
        last_error.map(|e| e.to_string())
    )))
}
