use bornemap_core::{AuthError, User, UserId};
use sqlx::PgPool;
use async_trait::async_trait;

pub struct PgUserRepository {
    pool: PgPool,
}

impl PgUserRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl bornemap_core::UserRepository for PgUserRepository {
    async fn create(&self, user: &User) -> Result<(), AuthError> {
        sqlx::query(
            "INSERT INTO users (id, email, password_hash, role, status, created_at) VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(user.id)
        .bind(&user.email)
        .bind(&user.password_hash)
        .bind(user.role.as_str())
        .bind(user.status.as_str())
        .bind(user.created_at)
        .execute(&self.pool)
        .await
        .map_err(|_| AuthError::InternalError)?;

        Ok(())
    }

    async fn find_by_email(&self, email: &str) -> Result<Option<User>, AuthError> {
        let rows = sqlx::query_as::<_, (UserId, String, String, String, String, chrono::DateTime<chrono::Utc>)>(
            "SELECT id, email, password_hash, role, status, created_at FROM users WHERE email = $1",
        )
        .bind(email)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| AuthError::InternalError)?;

        match rows {
            Some((id, email, password_hash, role_str, status_str, created_at)) => {
                let role = bornemap_core::UserRole::try_from_str(&role_str)
                    .ok_or(AuthError::InternalError)?;
                let status = bornemap_core::UserStatus::try_from_str(&status_str)
                    .ok_or(AuthError::InternalError)?;
                Ok(Some(User {
                    id,
                    email,
                    password_hash,
                    role,
                    status,
                    created_at,
                }))
            }
            None => Ok(None),
        }
    }

    async fn find_by_id(&self, id: UserId) -> Result<Option<User>, AuthError> {
        let rows = sqlx::query_as::<_, (UserId, String, String, String, String, chrono::DateTime<chrono::Utc>)>(
            "SELECT id, email, password_hash, role, status, created_at FROM users WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| AuthError::InternalError)?;

        match rows {
            Some((id, email, password_hash, role_str, status_str, created_at)) => {
                let role = bornemap_core::UserRole::try_from_str(&role_str)
                    .ok_or(AuthError::InternalError)?;
                let status = bornemap_core::UserStatus::try_from_str(&status_str)
                    .ok_or(AuthError::InternalError)?;
                Ok(Some(User {
                    id,
                    email,
                    password_hash,
                    role,
                    status,
                    created_at,
                }))
            }
            None => Ok(None),
        }
    }

    async fn email_exists(&self, email: &str) -> Result<bool, AuthError> {
        let row: Option<(bool,)> = sqlx::query_as(
            "SELECT EXISTS(SELECT 1 FROM users WHERE email = $1)",
        )
        .bind(email)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| AuthError::InternalError)?;

        Ok(row.map_or(false, |r| r.0))
    }
}
