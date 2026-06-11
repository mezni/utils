mod common;

use borne_data::run_migrations;

#[tokio::test]
async fn test_migration_applies_fresh() {
    let test_db = common::TestDb::new().await;

    run_migrations(&test_db.pool).await.unwrap();
}

#[tokio::test]
async fn test_migration_idempotent() {
    let test_db = common::TestDb::new().await;

    run_migrations(&test_db.pool).await.unwrap();
    run_migrations(&test_db.pool).await.unwrap();
}
