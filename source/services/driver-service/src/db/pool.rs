use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::time::Duration;

pub async fn create_pool(
    database_url: &str,
    min_connections: u32,
    max_connections: u32,
) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .min_connections(min_connections)
        .max_connections(max_connections)
        .connect(database_url)
        .await
}

pub async fn check_health(pool: &PgPool) -> bool {
    tokio::time::timeout(Duration::from_millis(500), pool.acquire())
        .await
        .ok()
        .and_then(|r| r.ok())
        .is_some()
}
