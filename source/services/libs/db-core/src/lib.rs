use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;

pub async fn create_platform_pool(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(20)
        .min_connections(5)
        .acquire_timeout(Duration::from_secs(3))
        .idle_timeout(Duration::from_secs(60))
        .connect(database_url)
        .await
}
