use sqlx::PgPool;
use testcontainers::{runners::AsyncRunner, ContainerAsync, GenericImage, ImageExt};

pub struct TestDb {
    pub pool: PgPool,
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
}

impl TestDb {
    pub async fn new() -> Self {
        let container = GenericImage::new("postgis/postgis", "16-3.4")
            .with_env_var("POSTGRES_DB", "platform_db")
            .with_env_var("POSTGRES_USER", "postgres")
            .with_env_var("POSTGRES_PASSWORD", "postgres")
            .start()
            .await
            .expect("Failed to start PostGIS container");

        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();

        let database_url = format!(
            "postgres://postgres:postgres@{}:{}/platform_db",
            host, port
        );

        let pool = loop {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            match PgPool::connect(&database_url).await {
                Ok(p) => {
                    let row: Result<(String,), _> =
                        sqlx::query_as("SELECT PostGIS_Version()").fetch_one(&p).await;
                    if row.is_ok() {
                        break p;
                    }
                }
                Err(_) => continue,
            }
        };

        Self { pool, container }
    }
}
