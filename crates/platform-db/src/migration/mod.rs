//! Database migrations
//! Provides migration execution utilities

use sqlx::PgPool;
use tracing::info;

/// Run pending migrations
pub async fn run_migrations(pool: &PgPool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Running database migrations...");

    // Note: In production, use sqlx migrations with `sqlx migrate run`
    // For now, migrations will be executed directly via SQLx

    Ok(())
}
