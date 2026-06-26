use bornemap_core::{AppError, Session, SessionRepository};
use chrono::NaiveDateTime;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Clone)]
pub struct PgSessionRepository {
    pool: PgPool,
}

impl PgSessionRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl SessionRepository for PgSessionRepository {
    async fn create(&self, session: &Session) -> Result<(), AppError> {
        sqlx::query(
            "INSERT INTO sessions (id, user_id, token_hash, family_id, created_at, expires_at, last_used_at, revoked, revoked_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(session.id)
        .bind(session.user_id)
        .bind(&session.token_hash)
        .bind(session.family_id)
        .bind(session.created_at)
        .bind(session.expires_at)
        .bind(session.last_used_at)
        .bind(session.revoked)
        .bind(session.revoked_at)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!("DB session create error: {:?}", e);
            AppError::DatabaseError(e.to_string())
        })?;

        Ok(())
    }

    async fn find_by_token_hash(&self, token_hash: &str) -> Result<Option<Session>, AppError> {
        let row = sqlx::query_as::<_, SessionRow>(
            "SELECT id, user_id, token_hash, family_id, created_at, expires_at, last_used_at, revoked, revoked_at \
             FROM sessions WHERE token_hash = $1",
        )
        .bind(token_hash)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!("DB session find error: {:?}", e);
            AppError::DatabaseError(e.to_string())
        })?;

        Ok(row.map(SessionRow::into_session))
    }

    async fn revoke_session(&self, id: Uuid) -> Result<(), AppError> {
        sqlx::query(
            "UPDATE sessions SET revoked = TRUE, revoked_at = NOW() WHERE id = $1",
        )
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!("DB session revoke error: {:?}", e);
            AppError::DatabaseError(e.to_string())
        })?;

        Ok(())
    }

    async fn revoke_family(&self, family_id: Uuid) -> Result<(), AppError> {
        sqlx::query(
            "UPDATE sessions SET revoked = TRUE, revoked_at = NOW() WHERE family_id = $1 AND revoked = FALSE",
        )
        .bind(family_id)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!("DB session family revoke error: {:?}", e);
            AppError::DatabaseError(e.to_string())
        })?;

        Ok(())
    }

    async fn delete_expired(&self) -> Result<u64, AppError> {
        let result = sqlx::query("DELETE FROM sessions WHERE expires_at < NOW()")
            .execute(&self.pool)
            .await
            .map_err(|e| {
                tracing::error!("DB session delete expired error: {:?}", e);
                AppError::DatabaseError(e.to_string())
            })?;

        Ok(result.rows_affected())
    }
}

#[derive(sqlx::FromRow)]
struct SessionRow {
    id: Uuid,
    user_id: Uuid,
    token_hash: String,
    family_id: Uuid,
    created_at: NaiveDateTime,
    expires_at: NaiveDateTime,
    last_used_at: NaiveDateTime,
    revoked: bool,
    revoked_at: Option<NaiveDateTime>,
}

impl SessionRow {
    fn into_session(self) -> Session {
        Session {
            id: self.id,
            user_id: self.user_id,
            token_hash: self.token_hash,
            family_id: self.family_id,
            created_at: self.created_at.and_utc(),
            expires_at: self.expires_at.and_utc(),
            last_used_at: self.last_used_at.and_utc(),
            revoked: self.revoked,
            revoked_at: self.revoked_at.map(|d| d.and_utc()),
        }
    }
}
