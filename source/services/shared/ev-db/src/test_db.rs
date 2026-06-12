use sqlx::postgres::{PgPool, PgPoolOptions};
use std::env;

pub struct TestDbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub db_name: String,
}

impl TestDbConfig {
    pub fn from_env() -> Self {
        Self {
            host: env::var("TEST_DB_HOST").unwrap_or_else(|_| "localhost".into()),
            port: env::var("TEST_DB_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(5434),
            user: env::var("TEST_DB_USER").unwrap_or_else(|_| "borneadmin".into()),
            password: env::var("TEST_DB_PASSWORD").unwrap_or_else(|_| "borne_dev_2026".into()),
            db_name: env::var("TEST_DB_NAME").unwrap_or_else(|_| "bornemap_test".into()),
        }
    }

    fn database_url(&self) -> String {
        format!(
            "postgresql://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.db_name
        )
    }

    fn admin_url(&self) -> String {
        format!(
            "postgresql://{}:{}@{}:{}/postgres",
            self.user, self.password, self.host, self.port
        )
    }
}

pub async fn setup_test_db(migrations_dir: &str) -> PgPool {
    let config = TestDbConfig::from_env();

    let admin_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&config.admin_url())
        .await
        .expect("Cannot connect to PostgreSQL admin database for test setup");

    sqlx::query(&format!(
        "SELECT pg_terminate_backend(pg_stat_activity.pid)
         FROM pg_stat_activity
         WHERE pg_stat_activity.datname = '{}'
           AND pid <> pg_backend_pid()",
        config.db_name
    ))
    .execute(&admin_pool)
    .await
    .ok();

    sqlx::query(&format!("DROP DATABASE IF EXISTS {}", config.db_name))
        .execute(&admin_pool)
        .await
        .expect("Failed to drop test database");

    sqlx::query(&format!("CREATE DATABASE {}", config.db_name))
        .execute(&admin_pool)
        .await
        .expect("Failed to create test database");

    admin_pool.close().await;

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&config.database_url())
        .await
        .expect("Cannot connect to test database after creation");

    run_migrations(&pool, migrations_dir).await;

    pool
}

async fn run_migrations(pool: &PgPool, dir: &str) {
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .expect("Migrations directory not found")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "sql"))
        .collect();

    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let path = entry.path();
        let sql = std::fs::read_to_string(&path)
            .unwrap_or_else(|_| panic!("Failed to read migration: {:?}", path));

        sqlx::query(&sql)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Migration {:?} failed: {}", path, e));
    }
}

pub async fn teardown_test_db(pool: PgPool) {
    pool.close().await;
}
