use bornemap_platform_db::pool::get_pool_with_retry;
use sqlx::PgPool;

pub async fn init_pool() -> PgPool {
    get_pool_with_retry(5, 3)
        .await
        .expect("Failed to connect to PostgreSQL")
}
