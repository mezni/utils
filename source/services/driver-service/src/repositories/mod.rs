use async_trait::async_trait;
use sqlx::PgPool;

use crate::error::DomainError;
use crate::models::Station;

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait StationRepository: Send + Sync {
    async fn find_all(&self) -> Result<Vec<Station>, DomainError>;
    async fn find_by_id(&self, id: &str) -> Result<Option<Station>, DomainError>;
    async fn find_nearby(
        &self,
        lat: f64,
        lng: f64,
        radius: f64,
    ) -> Result<Vec<Station>, DomainError>;
}

pub struct StationRepositoryImpl {
    db: PgPool,
}

impl StationRepositoryImpl {
    pub fn new(db: PgPool) -> Self {
        Self { db }
    }
}

#[async_trait]
impl StationRepository for StationRepositoryImpl {
    async fn find_all(&self) -> Result<Vec<Station>, DomainError> {
        use sqlx::Row;

        let rows = sqlx::query(
            r#"
            SELECT id, name, status, latitude, longitude, 0.0::double precision AS distance
            FROM inventory.station
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY id
            "#,
        )
        .fetch_all(&self.db)
        .await
        .map_err(|e| DomainError::Internal(format!("DB query failed: {}", e)))?;

        Ok(rows
            .iter()
            .map(|row| Station {
                id: row.get("id"),
                name: row.get::<Option<String>, _>("name").unwrap_or_default(),
                status: row.get("status"),
                latitude: row.get("latitude"),
                longitude: row.get("longitude"),
                distance: row.get::<f64, _>("distance"),
            })
            .collect())
    }

    async fn find_by_id(&self, id: &str) -> Result<Option<Station>, DomainError> {
        use sqlx::Row;

        let row = sqlx::query(
            r#"
            SELECT id, name, status, latitude, longitude, 0.0::double precision AS distance
            FROM inventory.station
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.db)
        .await
        .map_err(|e| DomainError::Internal(format!("DB query failed: {}", e)))?;

        Ok(row.map(|r| Station {
            id: r.get("id"),
            name: r.get::<Option<String>, _>("name").unwrap_or_default(),
            status: r.get("status"),
            latitude: r.get("latitude"),
            longitude: r.get("longitude"),
            distance: r.get::<f64, _>("distance"),
        }))
    }

    async fn find_nearby(
        &self,
        lat: f64,
        lng: f64,
        radius: f64,
    ) -> Result<Vec<Station>, DomainError> {
        use sqlx::Row;

        let rows = sqlx::query(
            r#"
            SELECT
                id, name, status, latitude, longitude,
                ST_Distance(location, ST_SetSRID(ST_MakePoint($2, $1), 4326)::geography) AS distance
            FROM inventory.station
            WHERE
                status = 'active'
                AND latitude IS NOT NULL
                AND longitude IS NOT NULL
                AND ST_DWithin(location, ST_SetSRID(ST_MakePoint($2, $1), 4326)::geography, $3)
            ORDER BY distance ASC
            "#,
        )
        .bind(lat)
        .bind(lng)
        .bind(radius)
        .fetch_all(&self.db)
        .await
        .map_err(|e| DomainError::Internal(format!("DB query failed: {}", e)))?;

        Ok(rows
            .iter()
            .map(|row| Station {
                id: row.get("id"),
                name: row.get::<Option<String>, _>("name").unwrap_or_default(),
                status: row.get("status"),
                latitude: row.get("latitude"),
                longitude: row.get("longitude"),
                distance: row.get::<f64, _>("distance"),
            })
            .collect())
    }
}
