use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::domain::user_profile::UserProfile;

#[derive(Debug, thiserror::Error)]
pub enum RepositoryError {
    #[error("not found")]
    NotFound,

    #[error("database error: {0}")]
    Database(String),
}

impl From<sqlx::Error> for RepositoryError {
    fn from(e: sqlx::Error) -> Self {
        match e {
            sqlx::Error::RowNotFound => RepositoryError::NotFound,
            _ => RepositoryError::Database(e.to_string()),
        }
    }
}

pub struct UserProfileRepository {
    pool: PgPool,
}

impl UserProfileRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn find_by_uuid(&self, user_uuid: Uuid) -> Result<UserProfile, RepositoryError> {
        let row = sqlx::query_as::<_, UserProfileRow>(
            r#"SELECT
                user_uuid, email, first_name, last_name, phone, locale,
                created_at, updated_at, deleted_at
               FROM users.user_profiles
               WHERE user_uuid = $1 AND deleted_at IS NULL"#,
        )
        .bind(user_uuid)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|r| r.into())
            .ok_or(RepositoryError::NotFound)
    }

    pub async fn find_by_email(&self, email: &str) -> Result<UserProfile, RepositoryError> {
        let row = sqlx::query_as::<_, UserProfileRow>(
            r#"SELECT
                user_uuid, email, first_name, last_name, phone, locale,
                created_at, updated_at, deleted_at
               FROM users.user_profiles
               WHERE email = $1 AND deleted_at IS NULL"#,
        )
        .bind(email)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|r| r.into())
            .ok_or(RepositoryError::NotFound)
    }

    pub async fn insert(
        &self,
        user_uuid: Uuid,
        email: &str,
        first_name: Option<&str>,
        last_name: Option<&str>,
    ) -> Result<UserProfile, RepositoryError> {
        let row = sqlx::query_as::<_, UserProfileRow>(
            r#"INSERT INTO users.user_profiles (user_uuid, email, first_name, last_name)
               VALUES ($1, $2, $3, $4)
               RETURNING
                user_uuid, email, first_name, last_name, phone, locale,
                created_at, updated_at, deleted_at"#,
        )
        .bind(user_uuid)
        .bind(email)
        .bind(first_name)
        .bind(last_name)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.into())
    }

    pub async fn update(
        &self,
        user_uuid: Uuid,
        first_name: Option<&str>,
        last_name: Option<&str>,
        phone: Option<&str>,
        locale: Option<&str>,
    ) -> Result<UserProfile, RepositoryError> {
        let row = sqlx::query_as::<_, UserProfileRow>(
            r#"UPDATE users.user_profiles
               SET
                first_name = COALESCE($1, first_name),
                last_name = COALESCE($2, last_name),
                phone = COALESCE($3, phone),
                locale = COALESCE($4, locale),
                updated_at = NOW()
               WHERE user_uuid = $5 AND deleted_at IS NULL
               RETURNING
                user_uuid, email, first_name, last_name, phone, locale,
                created_at, updated_at, deleted_at"#,
        )
        .bind(first_name)
        .bind(last_name)
        .bind(phone)
        .bind(locale)
        .bind(user_uuid)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.into())
    }
}

#[derive(sqlx::FromRow)]
struct UserProfileRow {
    user_uuid: Uuid,
    email: String,
    first_name: Option<String>,
    last_name: Option<String>,
    phone: Option<String>,
    locale: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    deleted_at: Option<DateTime<Utc>>,
}

impl From<UserProfileRow> for UserProfile {
    fn from(r: UserProfileRow) -> Self {
        Self {
            user_uuid: r.user_uuid,
            email: r.email,
            first_name: r.first_name,
            last_name: r.last_name,
            phone: r.phone,
            locale: r.locale,
            created_at: r.created_at,
            updated_at: r.updated_at,
            deleted_at: r.deleted_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_repository_error_display() {
        assert_eq!(RepositoryError::NotFound.to_string(), "not found");
        assert_eq!(
            RepositoryError::Database("connection failed".into()).to_string(),
            "database error: connection failed"
        );
    }

    #[test]
    fn test_user_profile_row_into_domain() {
        let uuid = Uuid::from_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let now = Utc::now();
        let row = UserProfileRow {
            user_uuid: uuid,
            email: "test@example.com".into(),
            first_name: Some("John".into()),
            last_name: Some("Doe".into()),
            phone: None,
            locale: Some("fr-TN".into()),
            created_at: now,
            updated_at: now,
            deleted_at: None,
        };
        let profile: UserProfile = row.into();
        assert_eq!(profile.user_uuid, uuid);
        assert_eq!(profile.email, "test@example.com");
        assert_eq!(profile.first_name, Some("John".into()));
        assert_eq!(profile.last_name, Some("Doe".into()));
        assert!(profile.phone.is_none());
        assert_eq!(profile.locale, Some("fr-TN".into()));
    }
}
