use bornemap_core::{AuthError, User, UserId, UserRepository, UserRole, UserStatus};
use chrono::NaiveDateTime;
use sqlx::PgPool;

#[derive(Clone)]
pub struct PgUserRepository {
    pool: PgPool,
}

impl PgUserRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl UserRepository for PgUserRepository {
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
        .map_err(|e| {
            if let sqlx::Error::Database(ref db_err) = e
                && let Some(code) = db_err.code()
                && code == "23505"
            {
                return AuthError::EmailAlreadyExists;
            }
            tracing::error!("DB create error: {:?}", e);
            AuthError::InternalError
        })?;

        Ok(())
    }

    async fn find_by_email(&self, email: &str) -> Result<Option<User>, AuthError> {
        let row = sqlx::query_as::<_, UserRow>(
            "SELECT id, email, password_hash, role, status, created_at FROM users WHERE email = $1",
        )
        .bind(email)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!("DB find_by_email error: {:?}", e);
            AuthError::InternalError
        })?;

        Ok(row.map(|r| r.into_user()))
    }

    async fn find_by_id(&self, id: UserId) -> Result<Option<User>, AuthError> {
        let row = sqlx::query_as::<_, UserRow>(
            "SELECT id, email, password_hash, role, status, created_at FROM users WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!("DB find_by_id error: {:?}", e);
            AuthError::InternalError
        })?;

        Ok(row.map(|r| r.into_user()))
    }

    async fn email_exists(&self, email: &str) -> Result<bool, AuthError> {
        let exists =
            sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM users WHERE email = $1)")
                .bind(email)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| {
                    tracing::error!("DB email_exists error: {:?}", e);
                    AuthError::InternalError
                })?;

        Ok(exists)
    }
}

#[derive(sqlx::FromRow)]
struct UserRow {
    id: UserId,
    email: String,
    password_hash: String,
    role: String,
    status: String,
    created_at: NaiveDateTime,
}

impl UserRow {
    fn into_user(self) -> User {
        User {
            id: self.id,
            email: self.email,
            password_hash: self.password_hash,
            role: UserRole::try_from_str(&self.role).unwrap_or(UserRole::RegisteredDriver),
            status: UserStatus::try_from_str(&self.status).unwrap_or(UserStatus::Active),
            created_at: self.created_at.and_utc(),
        }
    }
}
