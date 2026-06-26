use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use tracing::info;

async fn create_test_pool() -> sqlx::PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect_timeout(Duration::from_secs(10))
        .connect("postgresql://postgres:password@localhost:5432/auth_db")
        .await
        .expect("Failed to create test database pool");

    pool
}

#[tokio::test]
async fn test_database_connection() {
    let pool = create_test_pool().await;

    let result: i64 = sqlx::query_scalar("SELECT 1")
        .fetch_one(&pool)
        .await
        .expect("Query failed");

    assert_eq!(result, 1);
    info!("Database connection test passed");
}

#[tokio::test]
async fn test_users_table_exists() {
    let pool = create_test_pool().await;

    let result: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'users'",
    )
    .fetch_one(&pool)
    .await
    .expect("Query failed");

    assert_eq!(result, 1);
    info!("Users table exists test passed");
}

#[tokio::test]
async fn test_user_passwords_table_exists() {
    let pool = create_test_pool().await;

    let result: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'user_passwords'",
    )
    .fetch_one(&pool)
    .await
    .expect("Query failed");

    assert_eq!(result, 1);
    info!("User passwords table exists test passed");
}

#[tokio::test]
async fn test_refresh_tokens_table_exists() {
    let pool = create_test_pool().await;

    let result: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'refresh_tokens'",
    )
    .fetch_one(&pool)
    .await
    .expect("Query failed");

    assert_eq!(result, 1);
    info!("Refresh tokens table exists test passed");
}

#[tokio::test]
async fn test_login_audit_log_table_exists() {
    let pool = create_test_pool().await;

    let result: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'login_audit_log'",
    )
    .fetch_one(&pool)
    .await
    .expect("Query failed");

    assert_eq!(result, 1);
    info!("Login audit log table exists test passed");
}
