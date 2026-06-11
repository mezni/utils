mod common;

use std::time::Instant;

use borne_data::{find_by_id, find_nearby, list_all, run_migrations, DataLayerError};
use sqlx::PgPool;

fn setup_schema() -> &'static str {
    r#"
    CREATE SCHEMA IF NOT EXISTS inventory;

    CREATE TABLE IF NOT EXISTS inventory.partner (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        type TEXT NOT NULL CHECK (type IN ('business', 'personal')),
        is_verified BOOLEAN DEFAULT FALSE,
        is_active BOOLEAN DEFAULT TRUE,
        is_live BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        created_by TEXT,
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        updated_by TEXT
    );

    CREATE TABLE IF NOT EXISTS inventory.station (
        id TEXT PRIMARY KEY,
        partner_id TEXT NOT NULL REFERENCES inventory.partner(id) ON DELETE CASCADE,
        name TEXT NOT NULL,
        address TEXT,
        latitude NUMERIC(10,7) NOT NULL CHECK (latitude BETWEEN -90 AND 90),
        longitude NUMERIC(10,7) NOT NULL CHECK (longitude BETWEEN -180 AND 180),
        created_at TIMESTAMPTZ DEFAULT NOW(),
        created_by TEXT,
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        updated_by TEXT
    );

    CREATE TABLE IF NOT EXISTS inventory.charger (
        id TEXT PRIMARY KEY,
        station_id TEXT NOT NULL REFERENCES inventory.station(id) ON DELETE CASCADE,
        connector_type TEXT NOT NULL,
        power_kw NUMERIC(6,2) NOT NULL CHECK (power_kw > 0),
        status TEXT NOT NULL DEFAULT 'available',
        created_at TIMESTAMPTZ DEFAULT NOW(),
        created_by TEXT,
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        updated_by TEXT
    );
    "#
}

fn seed_data() -> &'static str {
    r#"
    INSERT INTO inventory.partner (id, name, type, is_verified, is_active, is_live)
    VALUES ('p1', 'Test Partner', 'business', true, true, true);

    INSERT INTO inventory.station (id, partner_id, name, address, latitude, longitude)
    VALUES
        ('s1', 'p1', 'Station Alpha', 'Tunis Centre', 36.8065, 10.1815),
        ('s2', 'p1', 'Station Beta', 'Lac 2', 36.8300, 10.1900),
        ('s3', 'p1', 'Station Gamma', 'Marsa', 36.8800, 10.3300);

    INSERT INTO inventory.charger (id, station_id, connector_type, power_kw, status)
    VALUES
        ('c1', 's1', 'CCS2', 150.0, 'available'),
        ('c2', 's1', 'Type2', 22.0, 'available'),
        ('c3', 's2', 'CCS2', 350.0, 'available'),
        ('c4', 's2', 'CHAdeMO', 50.0, 'occupied'),
        ('c5', 's3', 'Type2', 22.0, 'available');
    "#
}

async fn setup_db(pool: &PgPool) {
    sqlx::raw_sql(setup_schema())
        .execute(pool)
        .await
        .expect("Failed to create schema");
    sqlx::raw_sql(seed_data())
        .execute(pool)
        .await
        .expect("Failed to seed data");
}

#[tokio::test]
async fn test_stations_list_all_returns_seed_count() {
    let test_db = common::TestDb::new().await;
    setup_db(&test_db.pool).await;

    let stations = list_all(&test_db.pool).await.unwrap();
    assert_eq!(stations.len(), 3, "Expected 3 seed stations");
}

#[tokio::test]
async fn test_find_nearby_returns_ordered_results() {
    let test_db = common::TestDb::new().await;
    setup_db(&test_db.pool).await;

    let stations = find_nearby(&test_db.pool, 36.8065, 10.1815, 50_000.0)
        .await
        .unwrap();

    assert!(!stations.is_empty(), "Expected nearby stations within 50km");
    assert_eq!(stations[0].id, "s1", "Station Alpha should be closest");
}

#[tokio::test]
async fn test_find_nearby_empty_radius() {
    let test_db = common::TestDb::new().await;
    setup_db(&test_db.pool).await;

    let stations = find_nearby(&test_db.pool, 34.0, 10.0, 1000.0)
        .await
        .unwrap();

    assert!(stations.is_empty(), "Expected no stations in empty area");
}

#[tokio::test]
async fn test_find_by_id_returns_with_chargers_and_partner() {
    let test_db = common::TestDb::new().await;
    setup_db(&test_db.pool).await;

    let detail = find_by_id(&test_db.pool, "s1").await.unwrap();

    assert_eq!(detail.station.id, "s1");
    assert_eq!(detail.partner.id, "p1");
    assert_eq!(detail.chargers.len(), 2, "Station Alpha should have 2 chargers");
}

#[tokio::test]
async fn test_find_by_id_not_found() {
    let test_db = common::TestDb::new().await;
    setup_db(&test_db.pool).await;

    let result = find_by_id(&test_db.pool, "nonexistent").await;
    assert!(matches!(result, Err(DataLayerError::NotFound(_))));
}

#[tokio::test]
async fn test_benchmark_core_queries() {
    let test_db = common::TestDb::new().await;
    setup_db(&test_db.pool).await;
    run_migrations(&test_db.pool).await.unwrap();

    let start = Instant::now();

    let _ = list_all(&test_db.pool).await.unwrap();
    let _ = find_nearby(&test_db.pool, 36.8065, 10.1815, 50_000.0)
        .await
        .unwrap();
    let _ = find_by_id(&test_db.pool, "s1").await.unwrap();

    let elapsed = start.elapsed();
    assert!(
        elapsed.as_millis() < 10_000,
        "Core queries took {elapsed:?} (expected <10s cold)"
    );
}

#[tokio::test]
async fn test_connection_failure_returns_error() {
    let pool = match PgPool::connect("postgres://bad:creds@localhost:9999/platform_db").await {
        Ok(p) => p,
        Err(_) => return,
    };
    let result = list_all(&pool).await;
    assert!(result.is_err(), "Expected error from bad connection pool");
}
