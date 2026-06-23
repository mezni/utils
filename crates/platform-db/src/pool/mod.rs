//! Database pool management
//! Provides PostgreSQL connection pool management

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::env;
use tracing::error;

/// Create and configure PostgreSQL connection pool
///
/// # Returns
/// Configured connection pool or error
pub async fn create_pool() -> Result<PgPool, Box<dyn std::error::Error + Send + Sync>> {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = PgPoolOptions::new()
        .max_connections(20) // Default to 20 connections
        .connect(&database_url)
        .await?;

    tracing::info!("Connected to PostgreSQL database");

    Ok(pool)
}

/// Get connection pool with retry logic
pub async fn get_pool_with_retry(retries: u32, delay_secs: u64) -> Result<PgPool, Box<dyn std::error::Error + Send + Sync>> {
    let mut attempts = 0;
    loop {
        match create_pool().await {
            Ok(pool) => return Ok(pool),
            Err(e) => {
                attempts += 1;
                if attempts >= retries {
                    error!("Failed to connect to database after {} attempts: {}", retries, e);
                    return Err(e);
                }
                error!("Connection attempt {} failed: {}. Retrying in {} seconds...", attempts, e, delay_secs);
                tokio::time::sleep(tokio::time::Duration::from_secs(delay_secs)).await;
            }
        }
    }
}
