use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("Connection failed: {0}")]
    Connection(String),

    #[error("Pool exhausted")]
    PoolExhausted,

    #[error("Query failed: {0}")]
    Query(String),

    #[error("Migration failed: {0}")]
    Migration(String),

    #[error("Not found: {0}")]
    NotFound(String),
}

impl From<sqlx::Error> for DbError {
    fn from(e: sqlx::Error) -> Self {
        match e {
            sqlx::Error::PoolClosed | sqlx::Error::PoolTimedOut => DbError::PoolExhausted,
            sqlx::Error::RowNotFound => DbError::NotFound("row not found".into()),
            other => DbError::Query(other.to_string()),
        }
    }
}
