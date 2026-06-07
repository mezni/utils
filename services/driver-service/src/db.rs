// Database module
use ev_db::{PgPool, PostgresUrl};

/// Initialize database connection pool
pub fn create_pool(postgres_url: &PostgresUrl) -> Result<PgPool, String> {
    postgres_url
        .validate()
        .map_err(|e| format!("Invalid database URL: {}", e))?;

    // Create connection pool from the URL
    PgPool::new(postgres_url.as_str())
        .map_err(|e| format!("Failed to create database connection pool: {}", e))
}

/// Apply migrations on startup
pub async fn apply_migrations(pool: &PgPool) -> Result<(), String> {
    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .map_err(|e| format!("Failed to apply migrations: {}", e))?;

    tracing::info!("Database migrations applied successfully");

    Ok(())
}
