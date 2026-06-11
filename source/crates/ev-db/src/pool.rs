use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tracing::info;

use crate::DbError;

pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub name: String,
    pub max_connections: u32,
}

impl Default for DbConfig {
    fn default() -> Self {
        DbConfig {
            host: "localhost".into(),
            port: 5432,
            user: "postgres".into(),
            password: "postgres".into(),
            name: "platform_db".into(),
            max_connections: 10,
        }
    }
}

impl DbConfig {
    pub fn from_env() -> Self {
        DbConfig {
            host: std::env::var("DB_HOST").unwrap_or_else(|_| "localhost".into()),
            port: std::env::var("DB_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5432),
            user: std::env::var("DB_USER").unwrap_or_else(|_| "postgres".into()),
            password: std::env::var("DB_PASSWORD").unwrap_or_else(|_| "postgres".into()),
            name: std::env::var("DB_NAME").unwrap_or_else(|_| "platform_db".into()),
            max_connections: std::env::var("DB_MAX_CONNECTIONS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10),
        }
    }

    pub fn url(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.name
        )
    }
}

pub async fn create_pool() -> Result<PgPool, DbError> {
    create_pool_with_config(&DbConfig::from_env()).await
}

pub async fn create_pool_with_config(config: &DbConfig) -> Result<PgPool, DbError> {
    info!("Connecting to database at {}:{}", config.host, config.port);

    PgPoolOptions::new()
        .max_connections(config.max_connections)
        .connect(&config.url())
        .await
        .map_err(|e| DbError::Connection(e.to_string()))
}
