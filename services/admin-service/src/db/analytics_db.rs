//! Database connection pool for admin-service
//! Manages connection to analytics_db with proper configuration

use anyhow::Result;
use sqlx::postgres::{PgPool, PgPoolOptions};

/// Database connection pool configuration
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Database connection URL
    pub database_url: String,
    /// Maximum number of connections
    pub max_connections: u32,
    /// Connection timeout in seconds
    pub connection_timeout_secs: u64,
    /// Minimum connections to keep idle
    pub min_connections: u32,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            database_url: std::env::var("ANALYTICS_DB_URL").unwrap_or_else(|_| {
                "postgresql://admin_service:admin_service_password@localhost/analytics_db".to_string()
            }),
            max_connections: 10,
            connection_timeout_secs: 30,
            min_connections: 2,
        }
    }
}

/// Initialize database connection pool with configuration
///
/// # Arguments
/// * `config` - Database connection configuration
///
/// # Returns
/// Initialized PostgreSQL connection pool
pub async fn create_pool(config: DatabaseConfig) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(config.max_connections)
        .min_connections(config.min_connections)
        .acquire_timeout(std::time::Duration::from_secs(config.connection_timeout_secs))
        .connect(&config.database_url)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to analytics_db: {}", e))
}

/// Create default database pool
pub async fn create_default_pool() -> Result<PgPool> {
    create_pool(DatabaseConfig::default()).await
}

/// Test database connection health
///
/// # Arguments
/// * `pool` - PostgreSQL connection pool
///
/// # Returns
/// True if connection is healthy, false otherwise
pub async fn test_connection(pool: &PgPool) -> Result<bool> {
    sqlx::query("SELECT 1")
        .fetch_one(pool)
        .await
        .map(|_| true)
        .map_err(|e| {
            anyhow::anyhow!("Failed to test database connection: {}", e)
        })
}

/// Get database pool statistics (optional, for monitoring)
pub async fn get_pool_stats(pool: &PgPool) -> Result<PoolStats> {
    let version = sqlx::query("SELECT version()")
        .fetch_one(pool)
        .await?;

    let pool_size = pool.size();
    let idle_size = pool.idle_size();
    let acquired_size = pool.acquired_size();

    Ok(PoolStats {
        version: format!("{:?}", version),
        pool_size,
        idle_size,
        acquired_size,
    })
}

/// Pool statistics for monitoring
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub version: String,
    pub pool_size: usize,
    pub idle_size: usize,
    pub acquired_size: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = DatabaseConfig::default();
        assert_eq!(config.max_connections, 10);
        assert_eq!(config.connection_timeout_secs, 30);
        assert_eq!(config.min_connections, 2);
        assert!(!config.database_url.is_empty());
    }
}