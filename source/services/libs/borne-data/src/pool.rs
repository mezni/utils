use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::time::Duration;
use tracing::info;

use crate::error::DataLayerError;

pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub db_name: String,
    pub min_connections: u32,
    pub max_connections: u32,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            host: std::env::var("DB_HOST").unwrap_or_else(|_| "localhost".into()),
            port: std::env::var("DB_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5432),
            user: std::env::var("DB_USER").unwrap_or_else(|_| "postgres".into()),
            password: std::env::var("DB_PASSWORD").unwrap_or_else(|_| "postgres".into()),
            db_name: std::env::var("DB_NAME").unwrap_or_else(|_| "platform_db".into()),
            min_connections: std::env::var("DB_MIN_CONNECTIONS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(2),
            max_connections: std::env::var("DB_MAX_CONNECTIONS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10),
        }
    }
}

fn database_url_from_env() -> String {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        return url;
    }
    let config = DbConfig::default();
    format!(
        "postgres://{}:{}@{}:{}/{}",
        config.user, config.password, config.host, config.port, config.db_name
    )
}

pub async fn create_pool() -> Result<PgPool, DataLayerError> {
    let url = database_url_from_env();
    create_pool_with_url(&url, &DbConfig::default()).await
}

pub async fn create_pool_with_config(config: DbConfig) -> Result<PgPool, DataLayerError> {
    let url = format!(
        "postgres://{}:{}@{}:{}/{}",
        config.user, config.password, config.host, config.port, config.db_name
    );
    create_pool_with_url(&url, &config).await
}

async fn create_pool_with_url(url: &str, config: &DbConfig) -> Result<PgPool, DataLayerError> {
    let max_retries = 3;
    let mut last_error = None;

    for attempt in 1..=max_retries {
        match PgPoolOptions::new()
            .min_connections(config.min_connections)
            .max_connections(config.max_connections)
            .connect(url)
            .await
        {
            Ok(pool) => {
                info!(
                    "Connected to database at {}:{}, pool size {}/{}",
                    config.host, config.port, config.min_connections, config.max_connections
                );
                return Ok(pool);
            }
            Err(e) => {
                let delay = Duration::from_secs(2u64.pow(attempt - 1));
                info!(
                    "Database connection attempt {}/{} failed, retrying in {}s: {}",
                    attempt,
                    max_retries,
                    delay.as_secs(),
                    e
                );
                tokio::time::sleep(delay).await;
                last_error = Some(e);
            }
        }
    }

    Err(DataLayerError::Connection(format!(
        "Failed to connect after {} attempts: {:?}",
        max_retries,
        last_error
    )))
}
