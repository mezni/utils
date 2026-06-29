use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::env;

fn generate_id(prefix: &str) -> String {
    format!("{}_{}", prefix, nanoid::nanoid!(8).to_uppercase())
}

async fn get_pool() -> PgPool {
    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/bornemap_test".to_string());

    PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .expect("failed to connect to database")
}

async fn run_migrations(pool: &PgPool) {
    let migrations = vec![
        include_str!("../../../database/migrations/0001_create_ev_schema.sql"),
        include_str!("../../../database/migrations/0002_extensions.sql"),
        include_str!("../../../database/migrations/0003_create_partners.sql"),
        include_str!("../../../database/migrations/0004_create_stations.sql"),
        include_str!("../../../database/migrations/0005_create_connectors.sql"),
        include_str!("../../../database/migrations/0006_indexes.sql"),
        include_str!("../../../database/migrations/0007_updated_at_trigger.sql"),
        include_str!("../../../database/migrations/0008_updated_at_bindings.sql"),
    ];

    for migration in &migrations {
        sqlx::query(migration)
            .execute(pool)
            .await
            .expect("migration failed");
    }
}

#[sqlx::test]
async fn test_ev_schema_exists() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let row: (String,) = sqlx::query_as(
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'ev'",
    )
    .fetch_one(&pool)
    .await
    .expect("ev schema should exist");

    assert_eq!(row.0, "ev");
}

#[sqlx::test]
async fn test_create_partner() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let id = generate_id("PRT");
    sqlx::query("INSERT INTO ev.partners (id, name) VALUES ($1, $2)")
        .bind(&id)
        .bind("Tesla Tunisia")
        .execute(&pool)
        .await
        .expect("partner creation should succeed");

    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM ev.partners WHERE name = $1")
        .bind("Tesla Tunisia")
        .fetch_one(&pool)
        .await
        .expect("query should succeed");

    assert_eq!(count.0, 1);
}

#[sqlx::test]
async fn test_unique_partner_name() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let id1 = generate_id("PRT");
    sqlx::query("INSERT INTO ev.partners (id, name) VALUES ($1, $2)")
        .bind(&id1)
        .bind("Tesla")
        .execute(&pool)
        .await
        .expect("first insert should succeed");

    let id2 = generate_id("PRT");
    let result = sqlx::query("INSERT INTO ev.partners (id, name) VALUES ($1, $2)")
        .bind(&id2)
        .bind("Tesla")
        .execute(&pool)
        .await;

    assert!(result.is_err(), "duplicate partner name should be rejected");
}

#[sqlx::test]
async fn test_create_station() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let partner_id: String = sqlx::query_scalar(
        "INSERT INTO ev.partners (id, name) VALUES ($1, $2) RETURNING id",
    )
    .bind(generate_id("PRT"))
    .bind("Test Partner")
    .fetch_one(&pool)
    .await
    .expect("partner creation should succeed");

    sqlx::query(
        "INSERT INTO ev.stations (id, partner_id, name, address, latitude, longitude) VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(generate_id("STN"))
    .bind(partner_id)
    .bind("Station Alpha")
    .bind("123 Main St")
    .bind(36.8065)
    .bind(10.1815)
    .execute(&pool)
    .await
    .expect("station creation should succeed");

    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM ev.stations")
        .fetch_one(&pool)
        .await
        .expect("query should succeed");

    assert_eq!(count.0, 1);
}

#[sqlx::test]
async fn test_station_invalid_partner_fk() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let result = sqlx::query(
        "INSERT INTO ev.stations (id, partner_id, name, address, latitude, longitude) VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(generate_id("STN"))
    .bind("invalid_id")
    .bind("Orphan Station")
    .bind("Nowhere")
    .bind(0.0)
    .bind(0.0)
    .execute(&pool)
    .await;

    assert!(
        result.is_err(),
        "station with invalid partner FK should be rejected"
    );
}

#[sqlx::test]
async fn test_station_invalid_latitude() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let partner_id: String = sqlx::query_scalar(
        "INSERT INTO ev.partners (id, name) VALUES ($1, $2) RETURNING id",
    )
    .bind(generate_id("PRT"))
    .bind("Partner")
    .fetch_one(&pool)
    .await
    .expect("partner creation should succeed");

    let result = sqlx::query(
        "INSERT INTO ev.stations (id, partner_id, name, address, latitude, longitude) VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(generate_id("STN"))
    .bind(partner_id)
    .bind("Bad Station")
    .bind("Nowhere")
    .bind(100.0)
    .bind(0.0)
    .execute(&pool)
    .await;

    assert!(
        result.is_err(),
        "station with latitude > 90 should be rejected"
    );
}

#[sqlx::test]
async fn test_station_invalid_longitude() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let partner_id: String = sqlx::query_scalar(
        "INSERT INTO ev.partners (id, name) VALUES ($1, $2) RETURNING id",
    )
    .bind(generate_id("PRT"))
    .bind("Partner")
    .fetch_one(&pool)
    .await
    .expect("partner creation should succeed");

    let result = sqlx::query(
        "INSERT INTO ev.stations (id, partner_id, name, address, latitude, longitude) VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(generate_id("STN"))
    .bind(partner_id)
    .bind("Bad Station")
    .bind("Nowhere")
    .bind(0.0)
    .bind(200.0)
    .execute(&pool)
    .await;

    assert!(
        result.is_err(),
        "station with longitude > 180 should be rejected"
    );
}

#[sqlx::test]
async fn test_create_connector() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let partner_id: String = sqlx::query_scalar(
        "INSERT INTO ev.partners (id, name) VALUES ($1, $2) RETURNING id",
    )
    .bind(generate_id("PRT"))
    .bind("Partner")
    .fetch_one(&pool)
    .await
    .expect("partner creation should succeed");

    let station_id: String = sqlx::query_scalar(
        "INSERT INTO ev.stations (id, partner_id, name, address, latitude, longitude) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
    )
    .bind(generate_id("STN"))
    .bind(partner_id)
    .bind("Station")
    .bind("Addr")
    .bind(36.0)
    .bind(10.0)
    .fetch_one(&pool)
    .await
    .expect("station creation should succeed");

    sqlx::query(
        r#"INSERT INTO ev.connectors (id, station_id, "type", power_kw) VALUES ($1, $2, $3, $4)"#,
    )
    .bind(generate_id("CON"))
    .bind(station_id)
    .bind("CCS2")
    .bind(150.0)
    .execute(&pool)
    .await
    .expect("connector creation should succeed");

    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM ev.connectors")
        .fetch_one(&pool)
        .await
        .expect("query should succeed");

    assert_eq!(count.0, 1);
}

#[sqlx::test]
async fn test_connector_invalid_station_fk() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let result = sqlx::query(
        r#"INSERT INTO ev.connectors (id, station_id, "type", power_kw) VALUES ($1, $2, $3, $4)"#,
    )
    .bind(generate_id("CON"))
    .bind("invalid_station_id")
    .bind("CCS2")
    .bind(50.0)
    .execute(&pool)
    .await;

    assert!(
        result.is_err(),
        "connector with invalid station FK should be rejected"
    );
}

#[sqlx::test]
async fn test_connector_zero_power_rejected() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let partner_id: String = sqlx::query_scalar(
        "INSERT INTO ev.partners (id, name) VALUES ($1, $2) RETURNING id",
    )
    .bind(generate_id("PRT"))
    .bind("Partner")
    .fetch_one(&pool)
    .await
    .expect("partner creation should succeed");

    let station_id: String = sqlx::query_scalar(
        "INSERT INTO ev.stations (id, partner_id, name, address, latitude, longitude) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
    )
    .bind(generate_id("STN"))
    .bind(partner_id)
    .bind("Station")
    .bind("Addr")
    .bind(36.0)
    .bind(10.0)
    .fetch_one(&pool)
    .await
    .expect("station creation should succeed");

    let result = sqlx::query(
        r#"INSERT INTO ev.connectors (id, station_id, "type", power_kw) VALUES ($1, $2, $3, $4)"#,
    )
    .bind(generate_id("CON"))
    .bind(station_id)
    .bind("CCS2")
    .bind(0.0)
    .execute(&pool)
    .await;

    assert!(
        result.is_err(),
        "connector with zero power should be rejected"
    );
}

#[sqlx::test]
async fn test_cascade_delete_partner_removes_stations() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let partner_id: String = sqlx::query_scalar(
        "INSERT INTO ev.partners (id, name) VALUES ($1, $2) RETURNING id",
    )
    .bind(generate_id("PRT"))
    .bind("Delete Me")
    .fetch_one(&pool)
    .await
    .expect("partner creation should succeed");

    let station_id: String = sqlx::query_scalar(
        "INSERT INTO ev.stations (id, partner_id, name, address, latitude, longitude) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
    )
    .bind(generate_id("STN"))
    .bind(partner_id)
    .bind("Station To Delete")
    .bind("Addr")
    .bind(36.0)
    .bind(10.0)
    .fetch_one(&pool)
    .await
    .expect("station creation should succeed");

    sqlx::query(r#"INSERT INTO ev.connectors (id, station_id, "type", power_kw) VALUES ($1, $2, $3, $4)"#)
        .bind(generate_id("CON"))
        .bind(station_id)
        .bind("CCS2")
        .bind(50.0)
        .execute(&pool)
        .await
        .expect("connector creation should succeed");

    sqlx::query("DELETE FROM ev.partners WHERE id = $1")
        .bind(partner_id)
        .execute(&pool)
        .await
        .expect("delete should succeed");

    let station_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM ev.stations")
            .fetch_one(&pool)
            .await
            .expect("query should succeed");

    let connector_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM ev.connectors")
            .fetch_one(&pool)
            .await
            .expect("query should succeed");

    assert_eq!(station_count.0, 0, "stations should cascade delete");
    assert_eq!(
        connector_count.0, 0,
        "connectors should cascade delete through stations"
    );
}

#[sqlx::test]
async fn test_updated_at_trigger_on_partner() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let partner_id: String = sqlx::query_scalar(
        "INSERT INTO ev.partners (id, name) VALUES ($1, $2) RETURNING id",
    )
    .bind(generate_id("PRT"))
    .bind("Original Name")
    .fetch_one(&pool)
    .await
    .expect("partner creation should succeed");

    let (original_updated_at,): (chrono::DateTime<chrono::Utc>,) = sqlx::query_as(
        "SELECT updated_at FROM ev.partners WHERE id = $1",
    )
    .bind(&partner_id)
    .fetch_one(&pool)
    .await
    .expect("query should succeed");

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    sqlx::query("UPDATE ev.partners SET name = $1 WHERE id = $2")
        .bind("Updated Name")
        .bind(&partner_id)
        .execute(&pool)
        .await
        .expect("update should succeed");

    let (new_updated_at,): (chrono::DateTime<chrono::Utc>,) = sqlx::query_as(
        "SELECT updated_at FROM ev.partners WHERE id = $1",
    )
    .bind(&partner_id)
    .fetch_one(&pool)
    .await
    .expect("query should succeed");

    assert!(
        new_updated_at > original_updated_at,
        "updated_at should be greater after update"
    );
}

#[sqlx::test]
async fn test_migration_idempotency() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    run_migrations(&pool).await;

    let count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'ev'",
    )
    .fetch_one(&pool)
    .await
    .expect("query should succeed");

    assert_eq!(count.0, 3, "all 3 ev tables should exist after re-run");
}
