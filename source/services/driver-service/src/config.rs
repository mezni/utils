use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub host: String,
    pub port: u16,
    pub db_pool_size: u32,
    pub db_connect_retries: u32,
    pub db_retry_base_delay_ms: u64,
    pub rust_log: String,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            database_url: env::var("DATABASE_URL")
                .map_err(|_| ConfigError::Missing("DATABASE_URL".into()))?,
            host: env::var("HOST").unwrap_or_else(|_| "0.0.0.0".into()),
            port: env::var("PORT")
                .unwrap_or_else(|_| "3000".into())
                .parse()
                .map_err(|e| ConfigError::Parse("PORT".into(), e))?,
            db_pool_size: env::var("DB_POOL_SIZE")
                .unwrap_or_else(|_| "10".into())
                .parse()
                .map_err(|e| ConfigError::Parse("DB_POOL_SIZE".into(), e))?,
            db_connect_retries: env::var("DB_CONNECT_RETRIES")
                .unwrap_or_else(|_| "3".into())
                .parse()
                .map_err(|e| ConfigError::Parse("DB_CONNECT_RETRIES".into(), e))?,
            db_retry_base_delay_ms: env::var("DB_RETRY_BASE_DELAY_MS")
                .unwrap_or_else(|_| "1000".into())
                .parse()
                .map_err(|e| ConfigError::Parse("DB_RETRY_BASE_DELAY_MS".into(), e))?,
            rust_log: env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Missing required environment variable: {0}")]
    Missing(String),
    #[error("Failed to parse {0}: {1}")]
    Parse(String, std::num::ParseIntError),
}
