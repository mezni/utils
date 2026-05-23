use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::time::Duration;

pub struct Database {
    pool: PgPool,
}

impl Database {
    /// Create a new database connection pool
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(20)
            .min_connections(10)
            .acquire_timeout(Duration::from_secs(30))
            .idle_timeout(Duration::from_secs(600))
            .max_lifetime(Duration::from_secs(3600))
            .connect(database_url)
            .await?;

        Ok(Database { pool })
    }

    /// Get a reference to the connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Check database health
    pub async fn health_check(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .fetch_one(self.pool())
            .await
            .map_err(|e| anyhow::anyhow!("Database health check failed: {}", e))?;
        
        Ok(())
    }

    /// Get connection pool statistics
    pub async fn pool_stats(&self) -> PoolStats {
        let pool = self.pool();
        PoolStats {
            size: pool.size(),
            num_idle: pool.num_idle(),
            num_active: pool.num_active(),
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct PoolStats {
    pub size: u32,
    pub num_idle: u32,
    pub num_active: u32,
}

/// Create a database pool with retry logic for resilience
pub async fn create_pool_with_retry(database_url: &str) -> Result<PgPool> {
    let max_retries = 5;
    let retry_delay = Duration::from_secs(2);
    
    for attempt in 1..=max_retries {
        match PgPoolOptions::new()
            .max_connections(20)
            .min_connections(10)
            .acquire_timeout(Duration::from_secs(30))
            .idle_timeout(Duration::from_secs(600))
            .max_lifetime(Duration::from_secs(3600))
            .connect(database_url)
            .await
        {
            Ok(pool) => {
                log::info!("Database pool created successfully (attempt {})", attempt);
                return Ok(pool);
            }
            Err(e) => {
                if attempt == max_retries {
                    log::error!("Failed to create database pool after {} attempts: {}", max_retries, e);
                    return Err(e.into());
                }
                log::warn!("Database pool creation failed (attempt {}): {}, retrying...", attempt, e);
                tokio::time::sleep(retry_delay).await;
            }
        }
    }
    
    // This should never be reached due to the loop logic
    Err(anyhow::anyhow!("Unexpected error in database pool creation"))
}