use async_trait::async_trait;
use bornemap_platform_core::error::AppResult;
use sqlx::PgPool;

use crate::traits::DashboardRepository;

pub struct PgDashboardRepository {
    pool: PgPool,
}

impl PgDashboardRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DashboardRepository for PgDashboardRepository {
    async fn get_kpis(&self) -> AppResult<(i64, i64, i64)> {
        let row = sqlx::query_as::<_, (i64, i64, i64)>(
            r#"SELECT
                (SELECT COUNT(*) FROM ev.partners WHERE deleted_at IS NULL) as partners_count,
                (SELECT COUNT(*) FROM ev.stations WHERE deleted_at IS NULL) as stations_count,
                (SELECT COUNT(*) FROM ev.chargers WHERE deleted_at IS NULL) as chargers_count"#,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(row)
    }
}
