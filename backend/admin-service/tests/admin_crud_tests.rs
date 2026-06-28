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

// ---------------------------------------------------------------------------
// Geo validation unit tests
// ---------------------------------------------------------------------------
#[test]
fn test_geo_valid_lat_lon() {
    let geo = admin_service::domain::value_objects::geo::Geo::new(36.8065, 10.1815);
    assert!(geo.is_ok());
}

#[test]
fn test_geo_invalid_lat() {
    let geo = admin_service::domain::value_objects::geo::Geo::new(100.0, 10.0);
    assert!(geo.is_err());
}

#[test]
fn test_geo_invalid_lon() {
    let geo = admin_service::domain::value_objects::geo::Geo::new(36.0, 200.0);
    assert!(geo.is_err());
}

#[test]
fn test_geo_boundary_lat() {
    let geo = admin_service::domain::value_objects::geo::Geo::new(90.0, 0.0);
    assert!(geo.is_ok());
    let geo = admin_service::domain::value_objects::geo::Geo::new(-90.0, 0.0);
    assert!(geo.is_ok());
}

#[test]
fn test_geo_boundary_lon() {
    let geo = admin_service::domain::value_objects::geo::Geo::new(0.0, 180.0);
    assert!(geo.is_ok());
    let geo = admin_service::domain::value_objects::geo::Geo::new(0.0, -180.0);
    assert!(geo.is_ok());
}

// ---------------------------------------------------------------------------
// ID generation tests
// ---------------------------------------------------------------------------
#[test]
fn test_generated_ids_have_prefixes() {
    let pid = admin_service::domain::value_objects::ids::generate_partner_id();
    assert!(pid.starts_with("PRT_"));

    let sid = admin_service::domain::value_objects::ids::generate_station_id();
    assert!(sid.starts_with("STN_"));

    let cid = admin_service::domain::value_objects::ids::generate_connector_id();
    assert!(cid.starts_with("CON_"));
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------
#[sqlx::test]
async fn test_partner_repo_create() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let repo = admin_service::infrastructure::repositories::partner_repo::PostgresPartnerRepository::new(pool.clone());

    let partner = admin_service::domain::entities::partner::Partner {
        id: admin_service::domain::value_objects::ids::generate_partner_id(),
        name: "Test Partner".to_string(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };

    let result = repo.create(&partner).await;
    assert!(result.is_ok());
    let created = result.unwrap();
    assert_eq!(created.name, "Test Partner");
    assert!(created.id.starts_with("PRT_"));
}

#[sqlx::test]
async fn test_partner_repo_list() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let repo = admin_service::infrastructure::repositories::partner_repo::PostgresPartnerRepository::new(pool.clone());

    let partner = admin_service::domain::entities::partner::Partner {
        id: admin_service::domain::value_objects::ids::generate_partner_id(),
        name: "Listable Partner".to_string(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    repo.create(&partner).await.unwrap();

    let partners = repo.list().await.unwrap();
    assert!(!partners.is_empty());
    assert!(partners.iter().any(|p| p.name == "Listable Partner"));
}

#[sqlx::test]
async fn test_station_repo_create() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let partner_repo = admin_service::infrastructure::repositories::partner_repo::PostgresPartnerRepository::new(pool.clone());
    let station_repo = admin_service::infrastructure::repositories::station_repo::PostgresStationRepository::new(pool.clone());

    let partner = partner_repo
        .create(&admin_service::domain::entities::partner::Partner {
            id: admin_service::domain::value_objects::ids::generate_partner_id(),
            name: "Station Test Partner".to_string(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
        .await
        .unwrap();

    let station = admin_service::domain::entities::station::Station {
        id: admin_service::domain::value_objects::ids::generate_station_id(),
        partner_id: partner.id.clone(),
        name: "Test Station".to_string(),
        address: "123 Main St".to_string(),
        latitude: 36.8065,
        longitude: 10.1815,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };

    let result = station_repo.create(&station).await;
    assert!(result.is_ok());
    let created = result.unwrap();
    assert_eq!(created.name, "Test Station");
    assert_eq!(created.partner_id, partner.id);
}

#[sqlx::test]
async fn test_station_repo_delete_cascade() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let partner_repo = admin_service::infrastructure::repositories::partner_repo::PostgresPartnerRepository::new(pool.clone());
    let station_repo = admin_service::infrastructure::repositories::station_repo::PostgresStationRepository::new(pool.clone());
    let connector_repo =
        admin_service::infrastructure::repositories::connector_repo::PostgresConnectorRepository::new(pool.clone());

    let partner = partner_repo
        .create(&admin_service::domain::entities::partner::Partner {
            id: admin_service::domain::value_objects::ids::generate_partner_id(),
            name: "Cascade Delete Partner".to_string(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
        .await
        .unwrap();

    let station = station_repo
        .create(&admin_service::domain::entities::station::Station {
            id: admin_service::domain::value_objects::ids::generate_station_id(),
            partner_id: partner.id.clone(),
            name: "Cascade Station".to_string(),
            address: "Addr".to_string(),
            latitude: 36.0,
            longitude: 10.0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
        .await
        .unwrap();

    let _connector = connector_repo
        .create(&admin_service::domain::entities::connector::Connector {
            id: admin_service::domain::value_objects::ids::generate_connector_id(),
            station_id: station.id.clone(),
            connector_type: "CCS2".to_string(),
            power_kw: 150.0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
        .await
        .unwrap();

    // Delete station — should cascade delete connector
    station_repo.delete(&station.id).await.unwrap();

    let stations = station_repo.list(None).await.unwrap();
    assert!(!stations.iter().any(|s| s.id == station.id));

    let connectors = connector_repo.list_by_station(&station.id).await.unwrap();
    assert!(connectors.is_empty());
}

#[sqlx::test]
async fn test_connector_repo_create() {
    let pool = get_pool().await;
    run_migrations(&pool).await;

    let partner_repo = admin_service::infrastructure::repositories::partner_repo::PostgresPartnerRepository::new(pool.clone());
    let station_repo = admin_service::infrastructure::repositories::station_repo::PostgresStationRepository::new(pool.clone());
    let connector_repo =
        admin_service::infrastructure::repositories::connector_repo::PostgresConnectorRepository::new(pool.clone());

    let partner = partner_repo
        .create(&admin_service::domain::entities::partner::Partner {
            id: admin_service::domain::value_objects::ids::generate_partner_id(),
            name: "Connector Test Partner".to_string(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
        .await
        .unwrap();

    let station = station_repo
        .create(&admin_service::domain::entities::station::Station {
            id: admin_service::domain::value_objects::ids::generate_station_id(),
            partner_id: partner.id.clone(),
            name: "Connector Test Station".to_string(),
            address: "Addr".to_string(),
            latitude: 36.0,
            longitude: 10.0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
        .await
        .unwrap();

    let connector = admin_service::domain::entities::connector::Connector {
        id: admin_service::domain::value_objects::ids::generate_connector_id(),
        station_id: station.id.clone(),
        connector_type: "CCS2".to_string(),
        power_kw: 150.0,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };

    let result = connector_repo.create(&connector).await;
    assert!(result.is_ok());
    let created = result.unwrap();
    assert_eq!(created.connector_type, "CCS2");
    assert_eq!(created.station_id, station.id);
}
