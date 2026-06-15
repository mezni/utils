use sqlx::postgres::PgPool;

pub async fn init_pool(database_url: &str) -> PgPool {
    db_core::create_platform_pool(database_url)
        .await
        .expect("Failed to create database pool")
}
