use async_trait::async_trait;

use crate::domain::entities::partner::Partner;
use crate::domain::repositories::partner_repo::PartnerRepository;
use crate::infrastructure::db::pool::DbPool;

#[derive(Clone)]
pub struct PostgresPartnerRepository {
    pool: DbPool,
}

impl PostgresPartnerRepository {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PartnerRepository for PostgresPartnerRepository {
    async fn create(&self, partner: &Partner) -> Result<Partner, String> {
        sqlx::query_as::<_, (String, String, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
            r#"INSERT INTO ev.partners (id, name) VALUES ($1, $2)
               RETURNING id, name, created_at, updated_at"#,
        )
        .bind(&partner.id)
        .bind(&partner.name)
        .fetch_one(&self.pool)
        .await
        .map(|(id, name, created_at, updated_at)| Partner {
            id,
            name,
            created_at,
            updated_at,
        })
        .map_err(|e| format!("failed to create partner: {e}"))
    }

    async fn list(&self) -> Result<Vec<Partner>, String> {
        sqlx::query_as::<_, (String, String, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
            "SELECT id, name, created_at, updated_at FROM ev.partners ORDER BY created_at DESC",
        )
        .fetch_all(&self.pool)
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|(id, name, created_at, updated_at)| Partner {
                    id,
                    name,
                    created_at,
                    updated_at,
                })
                .collect()
        })
        .map_err(|e| format!("failed to list partners: {e}"))
    }

    async fn find_by_id(&self, id: &str) -> Result<Option<Partner>, String> {
        sqlx::query_as::<_, (String, String, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
            "SELECT id, name, created_at, updated_at FROM ev.partners WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map(|opt| {
            opt.map(|(id, name, created_at, updated_at)| Partner {
                id,
                name,
                created_at,
                updated_at,
            })
        })
        .map_err(|e| format!("failed to find partner: {e}"))
    }
}
