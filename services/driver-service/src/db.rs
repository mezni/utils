use sqlx::PgPool;

use crate::config::PostgresUrl;

pub async fn create_pool(postgres_url: &PostgresUrl) -> Result<PgPool, String> {
    postgres_url
        .validate()
        .map_err(|e| format!("Invalid database URL: {}", e))?;

    ev_db::create_pool(postgres_url.as_str())
        .await
        .map_err(|e| format!("Failed to create database connection pool: {}", e))
}

pub async fn apply_migrations(pool: &PgPool) -> Result<(), String> {
    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .map_err(|e| format!("Failed to apply migrations: {}", e))?;

    tracing::info!("Database migrations applied successfully");

    Ok(())
}
