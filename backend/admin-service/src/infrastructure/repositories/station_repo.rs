use async_trait::async_trait;

use crate::domain::entities::station::Station;
use crate::domain::repositories::station_repo::StationRepository;
use crate::infrastructure::db::pool::DbPool;

#[derive(Clone)]
pub struct PostgresStationRepository {
    pool: DbPool,
}

impl PostgresStationRepository {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl StationRepository for PostgresStationRepository {
    async fn create(&self, station: &Station) -> Result<Station, String> {
        sqlx::query_as::<_, (String, String, String, String, f64, f64, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
            r#"INSERT INTO ev.stations (id, partner_id, name, address, latitude, longitude)
               VALUES ($1, $2, $3, $4, $5, $6)
               RETURNING id, partner_id, name, address, latitude, longitude, created_at, updated_at"#,
        )
        .bind(&station.id)
        .bind(&station.partner_id)
        .bind(&station.name)
        .bind(&station.address)
        .bind(station.latitude)
        .bind(station.longitude)
        .fetch_one(&self.pool)
        .await
        .map(|(id, partner_id, name, address, latitude, longitude, created_at, updated_at)| {
            Station {
                id,
                partner_id,
                name,
                address,
                latitude,
                longitude,
                created_at,
                updated_at,
            }
        })
        .map_err(|e| format!("failed to create station: {e}"))
    }

    async fn list(&self, partner_id: Option<&str>) -> Result<Vec<Station>, String> {
        let query = if let Some(_pid) = partner_id {
            sqlx::query_as::<_, (String, String, String, String, f64, f64, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
                "SELECT id, partner_id, name, address, latitude, longitude, created_at, updated_at FROM ev.stations WHERE partner_id = $1 ORDER BY created_at DESC",
            )
            .bind(partner_id)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query_as::<_, (String, String, String, String, f64, f64, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
                "SELECT id, partner_id, name, address, latitude, longitude, created_at, updated_at FROM ev.stations ORDER BY created_at DESC",
            )
            .fetch_all(&self.pool)
            .await
        };

        query
            .map(|rows| {
                rows.into_iter()
                    .map(|(id, partner_id, name, address, latitude, longitude, created_at, updated_at)| {
                        Station {
                            id,
                            partner_id,
                            name,
                            address,
                            latitude,
                            longitude,
                            created_at,
                            updated_at,
                        }
                    })
                    .collect()
            })
            .map_err(|e| format!("failed to list stations: {e}"))
    }

    async fn find_by_id(&self, id: &str) -> Result<Option<Station>, String> {
        sqlx::query_as::<_, (String, String, String, String, f64, f64, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
            "SELECT id, partner_id, name, address, latitude, longitude, created_at, updated_at FROM ev.stations WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map(|opt| {
            opt.map(|(id, partner_id, name, address, latitude, longitude, created_at, updated_at)| {
                Station {
                    id,
                    partner_id,
                    name,
                    address,
                    latitude,
                    longitude,
                    created_at,
                    updated_at,
                }
            })
        })
        .map_err(|e| format!("failed to find station: {e}"))
    }

    async fn update(&self, station: &Station) -> Result<Station, String> {
        sqlx::query_as::<_, (String, String, String, String, f64, f64, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
            r#"UPDATE ev.stations
               SET name = $1, address = $2, latitude = $3, longitude = $4
               WHERE id = $5
               RETURNING id, partner_id, name, address, latitude, longitude, created_at, updated_at"#,
        )
        .bind(&station.name)
        .bind(&station.address)
        .bind(station.latitude)
        .bind(station.longitude)
        .bind(&station.id)
        .fetch_one(&self.pool)
        .await
        .map(|(id, partner_id, name, address, latitude, longitude, created_at, updated_at)| {
            Station {
                id,
                partner_id,
                name,
                address,
                latitude,
                longitude,
                created_at,
                updated_at,
            }
        })
        .map_err(|e| format!("failed to update station: {e}"))
    }

    async fn delete(&self, id: &str) -> Result<(), String> {
        sqlx::query("DELETE FROM ev.stations WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| format!("failed to delete station: {e}"))
    }
}
