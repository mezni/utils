use sqlx::PgPool;
use tracing::info;

use crate::error::DataLayerError;

pub async fn run_migrations(pool: &PgPool) -> Result<(), DataLayerError> {
    info!("Running database migrations...");
    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .map_err(|e| {
            DataLayerError::Migration(format!("Migration execution failed: {}", e))
        })?;
    info!("Database migrations completed successfully");
    Ok(())
}
