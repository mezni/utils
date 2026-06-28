use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::env;

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
    let migrations: Vec<&str> = vec![
        include_str!("../../../database/migrations/0001_create_ev_schema.sql"),
        include_str!("../../../database/migrations/0002_extensions.sql"),
        include_str!("../../../database/migrations/0003_create_partners.sql"),
        include_str!("../../../database/migrations/0004_create_stations.sql"),
        include_str!("../../../database/migrations/0005_create_connectors.sql"),
        include_str!("../../../database/migrations/0006_indexes.sql"),
        include_str!("../../../database/migrations/0007_updated_at_trigger.sql"),
        include_str!("../../../database/migrations/0008_updated_at_bindings.sql"),
        include_str!("../../../database/migrations/0009_enable_postgis.sql"),
        include_str!("../../../database/migrations/0010_create_gis_schema.sql"),
        include_str!("../../../database/migrations/0011_sync_trigger.sql"),
        include_str!("../../../database/migrations/0012_nearby_function.sql"),
    ];

    for migration in &migrations {
        sqlx::query(migration)
            .execute(pool)
            .await
            .expect("migration failed");
    }
}

#[sqlx::test]
async fn test_gis_schema_exists() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let row: (String,) = sqlx::query_as(
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'gis'",
    )
    .fetch_one(&pool)
    .await
    .expect("gis schema should exist");

    assert_eq!(row.0, "gis");
}

#[sqlx::test]
async fn test_station_insert_triggers_gis_projection() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let partner_id: String = sqlx::query_scalar(
        "INSERT INTO ev.partners (id, name) VALUES ($1, $2) RETURNING id",
    )
    .bind("PRT_TEST")
    .bind("GIS Test Partner")
    .fetch_one(&pool)
    .await
    .expect("partner creation should succeed");

    let station_id: String = sqlx::query_scalar(
        r#"INSERT INTO ev.stations (id, partner_id, name, address, latitude, longitude)
           VALUES ($1, $2, $3, $4, $5, $6) RETURNING id"#,
    )
    .bind("STN_GIS01")
    .bind(&partner_id)
    .bind("GIS Station")
    .bind("Tunis")
    .bind(36.8065)
    .bind(10.1815)
    .fetch_one(&pool)
    .await
    .expect("station creation should succeed");

    let (proj_id, lat, lon): (String, f64, f64) = sqlx::query_as(
        "SELECT station_id, latitude, longitude FROM gis.station_projection WHERE station_id = $1",
    )
    .bind(&station_id)
    .fetch_one(&pool)
    .await
    .expect("gis projection should exist after insert");

    assert_eq!(proj_id, station_id);
    assert!((lat - 36.8065).abs() < 0.0001);
    assert!((lon - 10.1815).abs() < 0.0001);
}

#[sqlx::test]
async fn test_station_update_syncs_gis_projection() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let partner_id: String = sqlx::query_scalar(
        "INSERT INTO ev.partners (id, name) VALUES ($1, $2) RETURNING id",
    )
    .bind("PRT_UPD")
    .bind("Update Partner")
    .fetch_one(&pool)
    .await
    .unwrap();

    let station_id: String = sqlx::query_scalar(
        r#"INSERT INTO ev.stations (id, partner_id, name, address, latitude, longitude)
           VALUES ($1, $2, $3, $4, $5, $6) RETURNING id"#,
    )
    .bind("STN_UPD01")
    .bind(&partner_id)
    .bind("Original")
    .bind("Addr")
    .bind(36.0)
    .bind(10.0)
    .fetch_one(&pool)
    .await
    .unwrap();

    sqlx::query("UPDATE ev.stations SET latitude = $1, longitude = $2 WHERE id = $3")
        .bind(48.8566)
        .bind(2.3522)
        .bind(&station_id)
        .execute(&pool)
        .await
        .unwrap();

    let (lat, lon): (f64, f64) = sqlx::query_as(
        "SELECT latitude, longitude FROM gis.station_projection WHERE station_id = $1",
    )
    .bind(&station_id)
    .fetch_one(&pool)
    .await
    .expect("gis projection should reflect update");

    assert!((lat - 48.8566).abs() < 0.0001);
    assert!((lon - 2.3522).abs() < 0.0001);
}

#[sqlx::test]
async fn test_station_delete_removes_gis_projection() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let partner_id: String = sqlx::query_scalar(
        "INSERT INTO ev.partners (id, name) VALUES ($1, $2) RETURNING id",
    )
    .bind("PRT_DEL")
    .bind("Delete Partner")
    .fetch_one(&pool)
    .await
    .unwrap();

    let station_id: String = sqlx::query_scalar(
        r#"INSERT INTO ev.stations (id, partner_id, name, address, latitude, longitude)
           VALUES ($1, $2, $3, $4, $5, $6) RETURNING id"#,
    )
    .bind("STN_DEL01")
    .bind(&partner_id)
    .bind("To Delete")
    .bind("Addr")
    .bind(36.0)
    .bind(10.0)
    .fetch_one(&pool)
    .await
    .unwrap();

    sqlx::query("DELETE FROM ev.stations WHERE id = $1")
        .bind(&station_id)
        .execute(&pool)
        .await
        .unwrap();

    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM gis.station_projection WHERE station_id = $1)",
    )
    .bind(&station_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert!(!exists, "gis projection should be removed after station delete");
}

#[sqlx::test]
async fn test_nearby_stations_no_results() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let rows: Vec<(String, f64, f64, f64)> = sqlx::query_as(
        "SELECT * FROM gis.get_nearby_stations($1, $2, $3)",
    )
    .bind(0.0)
    .bind(0.0)
    .bind(1000i32)
    .fetch_all(&pool)
    .await
    .expect("nearby function should execute");

    assert!(rows.is_empty(), "no stations should be near (0,0)");
}

#[sqlx::test]
async fn test_nearby_stations_returns_results() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let partner_id: String = sqlx::query_scalar(
        "INSERT INTO ev.partners (id, name) VALUES ($1, $2) RETURNING id",
    )
    .bind("PRT_NR")
    .bind("Nearby Partner")
    .fetch_one(&pool)
    .await
    .unwrap();

    sqlx::query(
        r#"INSERT INTO ev.stations (id, partner_id, name, address, latitude, longitude)
           VALUES ($1, $2, $3, $4, $5, $6)"#,
    )
    .bind("STN_NR01")
    .bind(&partner_id)
    .bind("Tunis Centre")
    .bind("Tunis")
    .bind(36.8065)
    .bind(10.1815)
    .execute(&pool)
    .await
    .unwrap();

    // Query from near the station location
    let rows: Vec<(String, f64, f64, f64)> = sqlx::query_as(
        "SELECT * FROM gis.get_nearby_stations($1, $2, $3)",
    )
    .bind(36.8070)
    .bind(10.1820)
    .bind(5000i32)
    .fetch_all(&pool)
    .await
    .expect("nearby function should execute");

    assert!(!rows.is_empty(), "should find nearby station");
    assert_eq!(rows[0].0, "STN_NR01");
    // Distance should be small (< 100m for 0.0005 deg offset)
    assert!(rows[0].3 < 100.0, "distance should be ~50m, got {}", rows[0].3);
}

#[sqlx::test]
async fn test_nearby_stations_filters_by_radius() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let partner_id: String = sqlx::query_scalar(
        "INSERT INTO ev.partners (id, name) VALUES ($1, $2) RETURNING id",
    )
    .bind("PRT_RAD")
    .bind("Radius Partner")
    .fetch_one(&pool)
    .await
    .unwrap();

    // Station far away (≈ Paris)
    sqlx::query(
        r#"INSERT INTO ev.stations (id, partner_id, name, address, latitude, longitude)
           VALUES ($1, $2, $3, $4, $5, $6)"#,
    )
    .bind("STN_RAD01")
    .bind(&partner_id)
    .bind("Paris Station")
    .bind("Paris")
    .bind(48.8566)
    .bind(2.3522)
    .execute(&pool)
    .await
    .unwrap();

    // Query from Tunis with 100m radius — should not find Paris
    let rows: Vec<(String, f64, f64, f64)> = sqlx::query_as(
        "SELECT * FROM gis.get_nearby_stations($1, $2, $3)",
    )
    .bind(36.8065)
    .bind(10.1815)
    .bind(100i32)
    .fetch_all(&pool)
    .await
    .expect("nearby function should execute");

    assert!(rows.is_empty(), "Paris station should not be within 100m of Tunis");
}

#[sqlx::test]
async fn test_sync_log_entries_created() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let partner_id: String = sqlx::query_scalar(
        "INSERT INTO ev.partners (id, name) VALUES ($1, $2) RETURNING id",
    )
    .bind("PRT_LOG")
    .bind("Log Partner")
    .fetch_one(&pool)
    .await
    .unwrap();

    sqlx::query(
        r#"INSERT INTO ev.stations (id, partner_id, name, address, latitude, longitude)
           VALUES ($1, $2, $3, $4, $5, $6)"#,
    )
    .bind("STN_LOG01")
    .bind(&partner_id)
    .bind("Log Station")
    .bind("Addr")
    .bind(36.0)
    .bind(10.0)
    .execute(&pool)
    .await
    .unwrap();

    let log_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM gis.station_projection_sync_log WHERE station_id = $1",
    )
    .bind("STN_LOG01")
    .fetch_one(&pool)
    .await
    .expect("log query should succeed");

    assert!(log_count.0 > 0, "sync log should have entries for insert");
}

#[sqlx::test]
async fn test_migration_idempotency() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    // Re-run — should not error
    run_migrations(&pool).await;

    let row: (String,) = sqlx::query_as(
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'gis'",
    )
    .fetch_one(&pool)
    .await
    .expect("gis schema should exist after re-run");

    assert_eq!(row.0, "gis");
}
