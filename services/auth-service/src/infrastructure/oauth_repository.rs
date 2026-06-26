use async_trait::async_trait;
use bornemap_core::{AppError, AuthError, User, UserId, UserRepository};
use bornemap_auth::OAuthProfile;
use sqlx::PgPool;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Clone)]
pub struct PgOAuthRepository {
    pool: PgPool,
}

impl PgOAuthRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn find_oauth_account(
        &self,
        provider: &str,
        provider_user_id: &str,
    ) -> Result<Option<OAuthAccount>, AppError> {
        let row = sqlx::query_as::<_, OAuthAccountRow>(
            "SELECT id, user_id, provider, provider_user_id, email, email_verified, first_name, last_name, avatar_url, created_at, updated_at 
             FROM oauth_accounts 
             WHERE provider = $1 AND provider_user_id = $2",
        )
        .bind(provider)
        .bind(provider_user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!("DB find_oauth_account error: {:?}", e);
            AppError::DatabaseError(e.to_string())
        })?;

        Ok(row.map(|r| r.into_oauth_account()))
    }

    pub async fn find_oauth_account_by_email(
        &self,
        provider: &str,
        email: &str,
    ) -> Result<Option<OAuthAccount>, AppError> {
        let row = sqlx::query_as::<_, OAuthAccountRow>(
            "SELECT id, user_id, provider, provider_user_id, email, email_verified, first_name, last_name, avatar_url, created_at, updated_at 
             FROM oauth_accounts 
             WHERE provider = $1 AND email = $2",
        )
        .bind(provider)
        .bind(email)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!("DB find_oauth_account_by_email error: {:?}", e);
            AppError::DatabaseError(e.to_string())
        })?;

        Ok(row.map(|r| r.into_oauth_account()))
    }

    pub async fn create_oauth_account(
        &self,
        user_id: UserId,
        profile: &OAuthProfile,
    ) -> Result<(), AppError> {
        let result = sqlx::query(
            "INSERT INTO oauth_accounts (user_id, provider, provider_user_id, email, email_verified, first_name, last_name, avatar_url, created_at, updated_at) 
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW())",
        )
        .bind(user_id)
        .bind(&profile.provider)
        .bind(&profile.provider_user_id)
        .bind(&profile.email)
        .bind(profile.email_verified)
        .bind(profile.first_name.as_ref())
        .bind(profile.last_name.as_ref())
        .bind(profile.avatar_url.as_ref())
        .execute(&self.pool)
        .await
        .map_err(|e| {
            if let sqlx::Error::Database(ref db_err) = e
                && let Some(code) = db_err.code()
                && code == "23505"
            {
                return AppError::OAuthAccountAlreadyExists;
            }
            tracing::error!("DB create_oauth_account error: {:?}", e);
            AppError::DatabaseError(e.to_string())
        })?;

        if result.rows_affected() == 0 {
            return Err(AppError::OAuthAccountLinkFailed("Failed to create OAuth account".to_string()));
        }

        Ok(())
    }

    pub async fn find_user_by_email(&self, email: &str) -> Result<Option<User>, AppError> {
        let row = sqlx::query_as::<_, UserRow>(
            "SELECT id, email, password_hash, role, status, created_at FROM users WHERE email = $1",
        )
        .bind(email)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!("DB find_user_by_email error: {:?}", e);
            AppError::DatabaseError(e.to_string())
        })?;

        Ok(row.map(|r| r.into_user()))
    }

    pub async fn link_oauth_account(
        &self,
        user_id: UserId,
        profile: &OAuthProfile,
    ) -> Result<(), AppError> {
        let result = sqlx::query(
            "INSERT INTO oauth_accounts (user_id, provider, provider_user_id, email, email_verified, first_name, last_name, avatar_url, created_at, updated_at) 
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW())",
        )
        .bind(user_id)
        .bind(&profile.provider)
        .bind(&profile.provider_user_id)
        .bind(&profile.email)
        .bind(profile.email_verified)
        .bind(profile.first_name.as_ref())
        .bind(profile.last_name.as_ref())
        .bind(profile.avatar_url.as_ref())
        .execute(&self.pool)
        .await
        .map_err(|e| {
            if let sqlx::Error::Database(ref db_err) = e
                && let Some(code) = db_err.code()
                && code == "23505"
            {
                return AppError::OAuthAccountAlreadyExists;
            }
            tracing::error!("DB link_oauth_account error: {:?}", e);
            AppError::DatabaseError(e.to_string())
        })?;

        if result.rows_affected() == 0 {
            return Err(AppError::OAuthAccountLinkFailed("Failed to link OAuth account".to_string()));
        }

        Ok(())
    }

    pub async fn create_user_with_oauth(
        &self,
        email: &str,
        profile: &OAuthProfile,
    ) -> Result<User, AppError> {
        let user_id = Uuid::new_v4();
        let user = User {
            id: user_id,
            email: email.to_string(),
            password_hash: String::new(), // OAuth users don't have passwords
            role: bornemap_core::UserRole::RegisteredDriver,
            status: bornemap_core::UserStatus::Active,
            created_at: chrono::Utc::now(),
        };

        // Insert the user
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
                return AppError::UserAlreadyExists;
            }
            tracing::error!("DB create_user_with_oauth error: {:?}", e);
            AppError::DatabaseError(e.to_string())
        })?;

        // Link the OAuth account
        self.link_oauth_account(user.id, profile).await?;

        Ok(user)
    }
}

#[derive(sqlx::FromRow)]
struct OAuthAccountRow {
    id: Uuid,
    user_id: Uuid,
    provider: String,
    provider_user_id: String,
    email: String,
    email_verified: bool,
    first_name: Option<String>,
    last_name: Option<String>,
    avatar_url: Option<String>,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
}

impl OAuthAccountRow {
    fn into_oauth_account(self) -> OAuthAccount {
        OAuthAccount {
            id: self.id,
            user_id: self.user_id,
            provider: self.provider,
            provider_user_id: self.provider_user_id,
            email: self.email,
            email_verified: self.email_verified,
            first_name: self.first_name,
            last_name: self.last_name,
            avatar_url: self.avatar_url,
            created_at: self.created_at.and_utc(),
            updated_at: self.updated_at.and_utc(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OAuthAccount {
    pub id: Uuid,
    pub user_id: Uuid,
    pub provider: String,
    pub provider_user_id: String,
    pub email: String,
    pub email_verified: bool,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub avatar_url: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(sqlx::FromRow)]
struct UserRow {
    id: UserId,
    email: String,
    password_hash: String,
    role: String,
    status: String,
    created_at: chrono::NaiveDateTime,
}

impl UserRow {
    fn into_user(self) -> User {
        User {
            id: self.id,
            email: self.email,
            password_hash: self.password_hash,
            role: bornemap_core::UserRole::try_from_str(&self.role).unwrap_or(bornemap_core::UserRole::RegisteredDriver),
            status: bornemap_core::UserStatus::try_from_str(&self.status).unwrap_or(bornemap_core::UserStatus::Active),
            created_at: self.created_at.and_utc(),
        }
    }
}