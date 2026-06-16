use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::time::Duration;

pub async fn init_pool(database_url: &str) -> PgPool {
    let mut attempts = 0;
    loop {
        match PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await
        {
            Ok(pool) => return pool,
            Err(e) => {
                attempts += 1;
                if attempts >= 30 {
                    panic!("Failed to create database pool after 30 retries: {e}");
                }
                log::warn!("DB connection attempt {attempts} failed: {e}. Retrying in 2s...");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}
