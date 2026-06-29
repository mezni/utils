use async_trait::async_trait;

use crate::domain::entities::connector::Connector;
use crate::domain::repositories::connector_repo::ConnectorRepository;
use crate::infrastructure::db::pool::DbPool;

#[derive(Clone)]
pub struct PostgresConnectorRepository {
    pool: DbPool,
}

impl PostgresConnectorRepository {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ConnectorRepository for PostgresConnectorRepository {
    async fn create(&self, connector: &Connector) -> Result<Connector, String> {
        sqlx::query_as::<_, (String, String, String, f64, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
            r#"INSERT INTO ev.connectors (id, station_id, "type", power_kw)
               VALUES ($1, $2, $3, $4)
               RETURNING id, station_id, "type", power_kw, created_at, updated_at"#,
        )
        .bind(&connector.id)
        .bind(&connector.station_id)
        .bind(&connector.connector_type)
        .bind(connector.power_kw)
        .fetch_one(&self.pool)
        .await
        .map(|(id, station_id, connector_type, power_kw, created_at, updated_at)| {
            Connector {
                id,
                station_id,
                connector_type,
                power_kw,
                created_at,
                updated_at,
            }
        })
        .map_err(|e| format!("failed to create connector: {e}"))
    }

    async fn list_by_station(&self, station_id: &str) -> Result<Vec<Connector>, String> {
        sqlx::query_as::<_, (String, String, String, f64, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
            r#"SELECT id, station_id, "type", power_kw, created_at, updated_at FROM ev.connectors WHERE station_id = $1 ORDER BY created_at ASC"#,
        )
        .bind(station_id)
        .fetch_all(&self.pool)
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|(id, station_id, connector_type, power_kw, created_at, updated_at)| {
                    Connector {
                        id,
                        station_id,
                        connector_type,
                        power_kw,
                        created_at,
                        updated_at,
                    }
                })
                .collect()
        })
        .map_err(|e| format!("failed to list connectors: {e}"))
    }

    async fn find_by_id(&self, id: &str) -> Result<Option<Connector>, String> {
        sqlx::query_as::<_, (String, String, String, f64, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
            r#"SELECT id, station_id, "type", power_kw, created_at, updated_at FROM ev.connectors WHERE id = $1"#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map(|opt| {
            opt.map(|(id, station_id, connector_type, power_kw, created_at, updated_at)| {
                Connector {
                    id,
                    station_id,
                    connector_type,
                    power_kw,
                    created_at,
                    updated_at,
                }
            })
        })
        .map_err(|e| format!("failed to find connector: {e}"))
    }

    async fn delete(&self, id: &str) -> Result<(), String> {
        sqlx::query("DELETE FROM ev.connectors WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| format!("failed to delete connector: {e}"))
    }
}
