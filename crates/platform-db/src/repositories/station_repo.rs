use async_trait::async_trait;
use bornemap_platform_core::error::AppResult;
use bornemap_platform_core::models::Station;
use chrono::Utc;
use sqlx::PgPool;

use crate::traits::StationRepository;

pub struct PgStationRepository {
    pool: PgPool,
}

impl PgStationRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl StationRepository for PgStationRepository {
    async fn create(
        &self,
        name: &str,
        location: Option<&str>,
        partner_id: &str,
        created_by: &str,
        updated_by: &str,
    ) -> AppResult<Station> {
        let id = format!("STA-{}", chrono::Utc::now().timestamp_nanos());
        let now = Utc::now();

        let station = sqlx::query_as::<_, Station>(
            r#"INSERT INTO ev.stations (id, partner_id, name, location, status, created_by, updated_by, created_at, updated_at)
               VALUES ($1, $2, $3, $4, 'ACTIVE', $5, $6, $7, $8)
               RETURNING id, partner_id, name, location, status, created_by, updated_by, created_at, updated_at, deleted_at"#,
        )
        .bind(&id)
        .bind(partner_id)
        .bind(name)
        .bind(location)
        .bind(created_by)
        .bind(updated_by)
        .bind(now)
        .bind(now)
        .fetch_one(&self.pool)
        .await?;

        Ok(station)
    }

    async fn get_by_id(&self, id: &str) -> AppResult<Option<Station>> {
        let station = sqlx::query_as::<_, Station>(
            r#"SELECT id, partner_id, name, location, status, created_by, updated_by, created_at, updated_at, deleted_at
               FROM ev.stations WHERE id = $1"#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(station)
    }

    async fn update(&self, id: &str, name: &str, location: Option<&str>, updated_by: &str) -> AppResult<Station> {
        let now = Utc::now();
        let station = sqlx::query_as::<_, Station>(
            r#"UPDATE ev.stations SET name = $1, location = $2, updated_at = $3, updated_by = $4
               WHERE id = $5
               RETURNING id, partner_id, name, location, status, created_by, updated_by, created_at, updated_at, deleted_at"#,
        )
        .bind(name)
        .bind(location)
        .bind(now)
        .bind(updated_by)
        .bind(id)
        .fetch_one(&self.pool)
        .await?;
        Ok(station)
    }

    async fn list(
        &self,
        page: u32,
        limit: u32,
        partner_id: Option<&str>,
    ) -> AppResult<(Vec<Station>, u64)> {
        let offset = ((page - 1).saturating_mul(limit)) as i64;

        let total: (i64,) = if let Some(pid) = partner_id {
            sqlx::query_as(
                r#"SELECT COUNT(*) FROM ev.stations WHERE deleted_at IS NULL AND partner_id = $1"#,
            )
            .bind(pid)
            .fetch_one(&self.pool)
            .await?
        } else {
            sqlx::query_as(
                r#"SELECT COUNT(*) FROM ev.stations WHERE deleted_at IS NULL"#,
            )
            .fetch_one(&self.pool)
            .await?
        };

        let stations = if let Some(pid) = partner_id {
            sqlx::query_as::<_, Station>(
                r#"SELECT id, partner_id, name, location, status, created_by, updated_by, created_at, updated_at, deleted_at
                   FROM ev.stations WHERE deleted_at IS NULL AND partner_id = $1
                   ORDER BY created_at DESC LIMIT $2 OFFSET $3"#,
            )
            .bind(pid)
            .bind(limit as i64)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as::<_, Station>(
                r#"SELECT id, partner_id, name, location, status, created_by, updated_by, created_at, updated_at, deleted_at
                   FROM ev.stations WHERE deleted_at IS NULL
                   ORDER BY created_at DESC LIMIT $1 OFFSET $2"#,
            )
            .bind(limit as i64)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?
        };

        Ok((stations, total.0 as u64))
    }

    async fn hard_delete(&self, id: &str) -> AppResult<()> {
        sqlx::query(r#"DELETE FROM ev.stations WHERE id = $1"#)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn soft_delete(&self, id: &str, updated_by: &str) -> AppResult<()> {
        let now = Utc::now();
        sqlx::query(
            r#"UPDATE ev.stations SET deleted_at = $1, updated_at = $1, updated_by = $2 WHERE id = $3"#,
        )
        .bind(now)
        .bind(updated_by)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn undelete(&self, id: &str, updated_by: &str) -> AppResult<Station> {
        let now = Utc::now();
        let station = sqlx::query_as::<_, Station>(
            r#"UPDATE ev.stations SET deleted_at = NULL, updated_at = $1, updated_by = $2
               WHERE id = $3
               RETURNING id, partner_id, name, location, status, created_by, updated_by, created_at, updated_at, deleted_at"#,
        )
        .bind(now)
        .bind(updated_by)
        .bind(id)
        .fetch_one(&self.pool)
        .await?;

        Ok(station)
    }
}
