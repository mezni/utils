use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataLayerError {
    #[error("Database connection failed: {0}")]
    Connection(String),

    #[error("Query execution failed: {0}")]
    Query(String),

    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Migration failed: {0}")]
    Migration(String),

    #[error("Connection pool exhausted")]
    PoolExhausted,
}
