use async_trait::async_trait;
use bornemap_platform_core::error::AppResult;
use bornemap_platform_core::models::Partner;
use chrono::Utc;
use sqlx::PgPool;

use crate::traits::PartnerRepository;

pub struct PgPartnerRepository {
    pool: PgPool,
}

impl PgPartnerRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PartnerRepository for PgPartnerRepository {
    async fn create(&self, name: &str, created_by: &str, updated_by: &str) -> AppResult<Partner> {
        let id = format!("PRT-{}", chrono::Utc::now().timestamp_nanos());
        let now = Utc::now();

        let partner = sqlx::query_as::<_, Partner>(
            r#"INSERT INTO ev.partners (id, name, status, is_valid, created_by, updated_by, created_at, updated_at)
               VALUES ($1, $2, 'ACTIVE', true, $3, $4, $5, $6)
               RETURNING id, name, status, is_valid, created_by, updated_by, created_at, updated_at, deleted_at"#,
        )
        .bind(&id)
        .bind(name)
        .bind(created_by)
        .bind(updated_by)
        .bind(now)
        .bind(now)
        .fetch_one(&self.pool)
        .await?;

        Ok(partner)
    }

    async fn get_by_id(&self, id: &str) -> AppResult<Option<Partner>> {
        let partner = sqlx::query_as::<_, Partner>(
            r#"SELECT id, name, status, is_valid, created_by, updated_by, created_at, updated_at, deleted_at
               FROM ev.partners WHERE id = $1"#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(partner)
    }

    async fn update(&self, id: &str, name: &str, updated_by: &str) -> AppResult<Partner> {
        let now = Utc::now();
        let partner = sqlx::query_as::<_, Partner>(
            r#"UPDATE ev.partners SET name = $1, updated_at = $2, updated_by = $3
               WHERE id = $4
               RETURNING id, name, status, is_valid, created_by, updated_by, created_at, updated_at, deleted_at"#,
        )
        .bind(name)
        .bind(now)
        .bind(updated_by)
        .bind(id)
        .fetch_one(&self.pool)
        .await?;
        Ok(partner)
    }

    async fn list(&self, page: u32, limit: u32) -> AppResult<(Vec<Partner>, u64)> {
        let offset = ((page - 1).saturating_mul(limit)) as i64;

        let total: (i64,) = sqlx::query_as(
            r#"SELECT COUNT(*) as count FROM ev.partners WHERE deleted_at IS NULL"#,
        )
        .fetch_one(&self.pool)
        .await?;

        let partners = sqlx::query_as::<_, Partner>(
            r#"SELECT id, name, status, is_valid, created_by, updated_by, created_at, updated_at, deleted_at
               FROM ev.partners WHERE deleted_at IS NULL
               ORDER BY created_at DESC LIMIT $1 OFFSET $2"#,
        )
        .bind(limit as i64)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        Ok((partners, total.0 as u64))
    }

    async fn hard_delete(&self, id: &str) -> AppResult<()> {
        sqlx::query(r#"DELETE FROM ev.partners WHERE id = $1"#)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn soft_delete(&self, id: &str, updated_by: &str) -> AppResult<()> {
        let now = Utc::now();
        sqlx::query(
            r#"UPDATE ev.partners SET deleted_at = $1, updated_at = $1, updated_by = $2 WHERE id = $3"#,
        )
        .bind(now)
        .bind(updated_by)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn undelete(&self, id: &str, updated_by: &str) -> AppResult<Partner> {
        let now = Utc::now();
        let partner = sqlx::query_as::<_, Partner>(
            r#"UPDATE ev.partners SET deleted_at = NULL, updated_at = $1, updated_by = $2
               WHERE id = $3
               RETURNING id, name, status, is_valid, created_by, updated_by, created_at, updated_at, deleted_at"#,
        )
        .bind(now)
        .bind(updated_by)
        .bind(id)
        .fetch_one(&self.pool)
        .await?;

        Ok(partner)
    }
}
