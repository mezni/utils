use async_trait::async_trait;
use bornemap_platform_core::error::AppResult;
use bornemap_platform_core::models::Charger;
use chrono::Utc;
use sqlx::PgPool;

use crate::traits::ChargerRepository;

pub struct PgChargerRepository {
    pool: PgPool,
}

impl PgChargerRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ChargerRepository for PgChargerRepository {
    async fn create(
        &self,
        station_id: &str,
        status: &str,
        power_rating: i32,
        created_by: &str,
        updated_by: &str,
    ) -> AppResult<Charger> {
        let id = format!("CHR-{}", chrono::Utc::now().timestamp_nanos());
        let now = Utc::now();

        let charger = sqlx::query_as::<_, Charger>(
            r#"INSERT INTO ev.chargers (id, station_id, status, power_rating, created_by, updated_by, created_at, updated_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               RETURNING id, station_id, status, power_rating, created_by, updated_by, created_at, updated_at, deleted_at"#,
        )
        .bind(&id)
        .bind(station_id)
        .bind(status)
        .bind(power_rating)
        .bind(created_by)
        .bind(updated_by)
        .bind(now)
        .bind(now)
        .fetch_one(&self.pool)
        .await?;

        Ok(charger)
    }

    async fn get_by_id(&self, id: &str) -> AppResult<Option<Charger>> {
        let charger = sqlx::query_as::<_, Charger>(
            r#"SELECT id, station_id, status, power_rating, created_by, updated_by, created_at, updated_at, deleted_at
               FROM ev.chargers WHERE id = $1"#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(charger)
    }

    async fn list(&self, page: u32, limit: u32, station_id: Option<&str>) -> AppResult<(Vec<Charger>, u64)> {
        let offset = ((page - 1).saturating_mul(limit)) as i64;

        let total: (i64,) = if let Some(sid) = station_id {
            sqlx::query_as(
                r#"SELECT COUNT(*) FROM ev.chargers WHERE deleted_at IS NULL AND station_id = $1"#,
            )
            .bind(sid)
            .fetch_one(&self.pool)
            .await?
        } else {
            sqlx::query_as(
                r#"SELECT COUNT(*) FROM ev.chargers WHERE deleted_at IS NULL"#,
            )
            .fetch_one(&self.pool)
            .await?
        };

        let chargers = if let Some(sid) = station_id {
            sqlx::query_as::<_, Charger>(
                r#"SELECT id, station_id, status, power_rating, created_by, updated_by, created_at, updated_at, deleted_at
                   FROM ev.chargers WHERE deleted_at IS NULL AND station_id = $1
                   ORDER BY created_at DESC LIMIT $2 OFFSET $3"#,
            )
            .bind(sid)
            .bind(limit as i64)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as::<_, Charger>(
                r#"SELECT id, station_id, status, power_rating, created_by, updated_by, created_at, updated_at, deleted_at
                   FROM ev.chargers WHERE deleted_at IS NULL
                   ORDER BY created_at DESC LIMIT $1 OFFSET $2"#,
            )
            .bind(limit as i64)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?
        };

        Ok((chargers, total.0 as u64))
    }

    async fn update_status(&self, id: &str, status: &str, updated_by: &str) -> AppResult<Charger> {
        let charger = sqlx::query_as::<_, Charger>(
            r#"UPDATE ev.chargers SET status = $1, updated_at = $2, updated_by = $3
               WHERE id = $4
               RETURNING id, station_id, status, power_rating, created_by, updated_by, created_at, updated_at, deleted_at"#,
        )
        .bind(status)
        .bind(Utc::now())
        .bind(updated_by)
        .bind(id)
        .fetch_one(&self.pool)
        .await?;

        Ok(charger)
    }

    async fn update_power_rating(&self, id: &str, power_rating: i32, updated_by: &str) -> AppResult<Charger> {
        let now = Utc::now();
        let charger = sqlx::query_as::<_, Charger>(
            r#"UPDATE ev.chargers SET power_rating = $1, updated_at = $2, updated_by = $3
               WHERE id = $4
               RETURNING id, station_id, status, power_rating, created_by, updated_by, created_at, updated_at, deleted_at"#,
        )
        .bind(power_rating)
        .bind(now)
        .bind(updated_by)
        .bind(id)
        .fetch_one(&self.pool)
        .await?;

        Ok(charger)
    }

    async fn hard_delete(&self, id: &str) -> AppResult<()> {
        sqlx::query(r#"DELETE FROM ev.chargers WHERE id = $1"#)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn soft_delete(&self, id: &str, updated_by: &str) -> AppResult<()> {
        let now = Utc::now();
        sqlx::query(
            r#"UPDATE ev.chargers SET deleted_at = $1, updated_at = $1, updated_by = $2 WHERE id = $3"#,
        )
        .bind(now)
        .bind(updated_by)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn undelete(&self, id: &str, updated_by: &str) -> AppResult<Charger> {
        let now = Utc::now();
        let charger = sqlx::query_as::<_, Charger>(
            r#"UPDATE ev.chargers SET deleted_at = NULL, updated_at = $1, updated_by = $2
               WHERE id = $3
               RETURNING id, station_id, status, power_rating, created_by, updated_by, created_at, updated_at, deleted_at"#,
        )
        .bind(now)
        .bind(updated_by)
        .bind(id)
        .fetch_one(&self.pool)
        .await?;

        Ok(charger)
    }
}
