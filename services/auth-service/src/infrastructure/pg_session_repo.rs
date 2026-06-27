use bornemap_core::{AppError, Session, SessionId, UserId};
use sqlx::PgPool;
use async_trait::async_trait;
use uuid::Uuid;

pub struct PgSessionRepository {
    pool: PgPool,
}

impl PgSessionRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl bornemap_core::SessionRepository for PgSessionRepository {
    async fn create(&self, session: &Session) -> Result<(), AppError> {
        sqlx::query(
            "INSERT INTO sessions (id, user_id, token_hash, family_id, created_at, expires_at, last_used_at, revoked, revoked_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
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
        .map_err(|e| AppError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    async fn find_by_token_hash(&self, token_hash: &str) -> Result<Option<Session>, AppError> {
        let rows = sqlx::query_as::<_, (SessionId, UserId, String, Uuid, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>, bool, Option<chrono::DateTime<chrono::Utc>>)>(
            "SELECT id, user_id, token_hash, family_id, created_at, expires_at, last_used_at, revoked, revoked_at FROM sessions WHERE token_hash = $1",
        )
        .bind(token_hash)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| AppError::DatabaseError(e.to_string()))?;

        match rows {
            Some((id, user_id, token_hash, family_id, created_at, expires_at, last_used_at, revoked, revoked_at)) => {
                Ok(Some(Session {
                    id,
                    user_id,
                    token_hash,
                    family_id,
                    created_at,
                    expires_at,
                    last_used_at,
                    revoked,
                    revoked_at,
                }))
            }
            None => Ok(None),
        }
    }

    async fn revoke_session(&self, id: SessionId) -> Result<(), AppError> {
        sqlx::query("UPDATE sessions SET revoked = true, revoked_at = NOW() WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| AppError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    async fn revoke_family(&self, family_id: Uuid) -> Result<(), AppError> {
        sqlx::query("UPDATE sessions SET revoked = true, revoked_at = NOW() WHERE family_id = $1")
            .bind(family_id)
            .execute(&self.pool)
            .await
            .map_err(|e| AppError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    async fn delete_user_sessions(&self, user_id: UserId) -> Result<(), AppError> {
        sqlx::query("DELETE FROM sessions WHERE user_id = $1")
            .bind(user_id)
            .execute(&self.pool)
            .await
            .map_err(|e| AppError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    async fn delete_expired(&self) -> Result<u64, AppError> {
        let result = sqlx::query("DELETE FROM sessions WHERE expires_at < NOW()")
            .execute(&self.pool)
            .await
            .map_err(|e| AppError::DatabaseError(e.to_string()))?;

        Ok(result.rows_affected() as u64)
    }
}
