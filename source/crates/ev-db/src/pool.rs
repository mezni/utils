use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::time::Duration;

/// Errors that can occur during pool initialization.
#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    /// The provided connection string is invalid or missing required fields.
    #[error("invalid connection string: {0}")]
    InvalidConnectionString(String),
    /// The database connection could not be established.
    #[error("database connection failed: {0}")]
    ConnectionFailed(#[from] sqlx::Error),
}

/// Configuration for creating a PostgreSQL connection pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// PostgreSQL connection URI (e.g., `postgres://user:pass@host:port/dbname`).
    pub connection_string: String,
    /// Maximum number of connections in the pool.
    pub max_connections: u32,
    /// Timeout duration for establishing a new connection.
    pub connection_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            connection_string: String::new(),
            max_connections: 10,
            connection_timeout: Duration::from_secs(30),
        }
    }
}

/// Initializes a [`PgPool`] from a PostgreSQL connection URI.
///
/// # Errors
///
/// Returns `PoolError::InvalidConnectionString` if the string is empty or
/// does not start with `postgres://`. Returns `PoolError::ConnectionFailed`
/// if the pool cannot connect to the database.
///
/// # Example
///
/// ```ignore
/// use ev_db::init_pool;
/// let pool = init_pool("postgres://user:pass@localhost:5432/borne_map").await?;
/// ```
pub async fn init_pool(connection_string: &str) -> Result<PgPool, PoolError> {
    validate_connection_string(connection_string)?;
    PgPoolOptions::new()
        .max_connections(10)
        .connect(connection_string)
        .await
        .map_err(PoolError::ConnectionFailed)
}

/// Initializes a [`PgPool`] from a [`PoolConfig`], allowing control over
/// pool size and connection timeout.
///
/// # Errors
///
/// Same as [`init_pool`].
///
/// # Example
///
/// ```ignore
/// use ev_db::{init_pool_with_config, PoolConfig};
/// use std::time::Duration;
///
/// let config = PoolConfig {
///     connection_string: "postgres://user:pass@localhost:5432/borne_map".into(),
///     max_connections: 20,
///     connection_timeout: Duration::from_secs(15),
/// };
/// let pool = init_pool_with_config(config).await?;
/// ```
pub async fn init_pool_with_config(config: PoolConfig) -> Result<PgPool, PoolError> {
    validate_connection_string(&config.connection_string)?;
    PgPoolOptions::new()
        .max_connections(config.max_connections)
        .acquire_timeout(config.connection_timeout)
        .connect(&config.connection_string)
        .await
        .map_err(PoolError::ConnectionFailed)
}

fn validate_connection_string(s: &str) -> Result<(), PoolError> {
    if s.is_empty() {
        return Err(PoolError::InvalidConnectionString(
            "connection string is empty".into(),
        ));
    }
    if !s.starts_with("postgres://") && !s.starts_with("postgresql://") {
        return Err(PoolError::InvalidConnectionString(
            format!("connection string must start with 'postgres://' or 'postgresql://', got: {s}"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_connection_string_returns_error() {
        let err = validate_connection_string("").unwrap_err();
        assert!(matches!(err, PoolError::InvalidConnectionString(_)));
    }

    #[test]
    fn invalid_scheme_returns_error() {
        let err = validate_connection_string("mysql://user:pass@localhost/db").unwrap_err();
        assert!(matches!(err, PoolError::InvalidConnectionString(_)));
        assert!(format!("{err}").contains("postgres://"));
    }

    #[test]
    fn valid_connection_string_passes_validation() {
        let result = validate_connection_string("postgres://user:pass@localhost:5432/borne_map");
        assert!(result.is_ok());
    }

    #[test]
    fn valid_postgresql_scheme_passes_validation() {
        let result = validate_connection_string("postgresql://user:pass@localhost:5432/borne_map");
        assert!(result.is_ok());
    }

    #[test]
    fn pool_config_defaults() {
        let config = PoolConfig::default();
        assert_eq!(config.max_connections, 10);
        assert_eq!(config.connection_timeout, Duration::from_secs(30));
        assert!(config.connection_string.is_empty());
    }
}
