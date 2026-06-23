//! Transaction management utilities
//! Provides transaction execution helpers

use sqlx::PgPool;
use sqlx::Transaction;
use tracing::{info, warn};

/// Execute a function within a database transaction
pub async fn with_transaction<F, T, E>(
    pool: &PgPool,
    f: F,
) -> Result<T, E>
where
    F: FnOnce(Transaction<'_, sqlx::Postgres>) -> Result<T, E>,
    E: std::error::Error + Send + Sync,
{
    let mut tx = pool.begin().await?;
    let result = f(tx)?;
    tx.commit().await?;
    Ok(result)
}

/// Execute a function within a transaction with error rollback
pub async fn transaction_with_rollback<F, T, E>(
    pool: &PgPool,
    f: F,
) -> Result<T, E>
where
    F: FnOnce(Transaction<'_, sqlx::Postgres>) -> Result<T, E>,
    E: std::error::Error + Send + Sync,
{
    let mut tx = pool.begin().await?;
    let result = f(tx)?;
    if let Err(e) = tx.commit().await {
        warn!("Transaction commit failed: {}", e);
        // Transaction will auto-rollback on drop
    }
    Ok(result)
}
