use sqlx::PgPool;

// Helper to get test database URL
fn test_db_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://bornemap:bornemap_dev@localhost:5432/platform_db_test".into())
}

async fn setup_pool() -> PgPool {
    let pool = PgPool::connect(&test_db_url())
        .await
        .expect("Failed to connect to test database");
    sqlx::migrate!("../migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");
    pool
}

#[sqlx::test]
async fn test_partner_crud() {
    let pool = setup_pool().await;

    // Create
    let partner = sqlx::query!(
        r#"
        INSERT INTO inventory.partners (id, name, network_type)
        VALUES ($1, $2, $3)
        RETURNING id, name, network_type, is_verified, deleted_at
        "#,
        "OPR-Test12345678",
        "Test Partner",
        "INDIVIDUAL"
    )
    .fetch_one(&pool)
    .await
    .expect("Failed to create partner");

    assert_eq!(partner.name, "Test Partner");
    assert_eq!(partner.network_type, Some("INDIVIDUAL".into()));
    assert!(!partner.is_verified.unwrap_or(false));
    assert!(partner.deleted_at.is_none());
}

#[sqlx::test]
async fn test_station_spatial() {
    let pool = setup_pool().await;

    // Create partner first
    sqlx::query!(
        r#"
        INSERT INTO inventory.partners (id, name, network_type)
        VALUES ($1, $2, $3)
        "#,
        "OPR-StationTest01",
        "Station Test Partner",
        "COMPANY"
    )
    .execute(&pool)
    .await
    .expect("Failed to create partner");

    // Create station with spatial data
    let station = sqlx::query!(
        r#"
        INSERT INTO inventory.stations (id, partner_id, name, location)
        VALUES ($1, $2, $3, ST_GeogFromText($4))
        RETURNING id, name, ST_AsText(location) AS location_str
        "#,
        "STA-LocTest9876",
        "OPR-StationTest01",
        "Test Station",
        "POINT(10.0 36.0)"
    )
    .fetch_one(&pool)
    .await
    .expect("Failed to create station");

    assert_eq!(station.name, "Test Station");
    assert!(station.location_str.contains("POINT"));
}

#[sqlx::test]
async fn test_charger_unique_constraint() {
    let pool = setup_pool().await;

    // Create partner and station
    sqlx::query!("INSERT INTO inventory.partners (id, name, network_type) VALUES ($1, $2, $3)",
        "OPR-ChargerUniq", "Charger Test", "INDIVIDUAL")
        .execute(&pool).await.unwrap();
    sqlx::query!("INSERT INTO inventory.stations (id, partner_id, name, location) VALUES ($1, $2, $3, ST_GeogFromText($4))",
        "STA-ChargerUniq", "OPR-ChargerUniq", "Charger Station", "POINT(10.0 36.0)")
        .execute(&pool).await.unwrap();

    // Create first charger
    let result1 = sqlx::query!(
        r#"
        INSERT INTO inventory.chargers (id, station_id, connector_type_id, current_type_id, status_id)
        VALUES ($1, $2, $3, $4, $5)
        "#,
        "CHG-UniqType01",
        "STA-ChargerUniq",
        1i64,
        1i64,
        1i64
    )
    .execute(&pool)
    .await;
    assert!(result1.is_ok(), "First charger should succeed");

    // Attempt duplicate connector_type at same station
    let result2 = sqlx::query!(
        r#"
        INSERT INTO inventory.chargers (id, station_id, connector_type_id, current_type_id, status_id)
        VALUES ($1, $2, $3, $4, $5)
        "#,
        "CHG-UniqType02",
        "STA-ChargerUniq",
        1i64,
        2i64,
        1i64
    )
    .execute(&pool)
    .await;
    assert!(result2.is_err(), "Duplicate connector_type should fail");
}
