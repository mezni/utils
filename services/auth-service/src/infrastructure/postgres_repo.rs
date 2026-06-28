use async_trait::async_trait;
use sqlx::PgPool;
use uuid::Uuid;

use crate::domain::account::Account;
use crate::domain::error::DomainError;
use crate::domain::repository::AccountRepository;

#[derive(Clone)]
pub struct PostgresAccountRepository {
    pool: PgPool,
}

impl PostgresAccountRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl AccountRepository for PostgresAccountRepository {
    async fn create(&self, account: &Account) -> Result<(), DomainError> {
        let result = sqlx::query(
            r#"INSERT INTO users.accounts (id, email, password_hash, role, created_at)
               VALUES ($1, $2, $3, $4, $5)"#,
        )
        .bind(account.id)
        .bind(&account.email)
        .bind(&account.password_hash)
        .bind(&account.role)
        .bind(account.created_at)
        .execute(&self.pool)
        .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                if let Some(db_err) = e.as_database_error() {
                    if let Some(code) = db_err.code() {
                        if code.as_ref() == "23505" {
                            return Err(DomainError::AlreadyExists);
                        }
                    }
                }
                Err(DomainError::InvalidCredentials)
            }
        }
    }

    async fn find_by_email(&self, email: &str) -> Result<Option<Account>, DomainError> {
        let row = sqlx::query_as::<_, (Uuid, String, String, String, chrono::DateTime<chrono::Utc>)>(
            r#"SELECT id, email, password_hash, role, created_at
               FROM users.accounts
               WHERE email = $1"#,
        )
        .bind(email)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| DomainError::NotFound)?;

        Ok(row.map(|(id, email, password_hash, role, created_at)| Account {
            id,
            email,
            password_hash,
            role,
            created_at,
        }))
    }

    async fn find_by_id(&self, id: Uuid) -> Result<Option<Account>, DomainError> {
        let row = sqlx::query_as::<_, (Uuid, String, String, String, chrono::DateTime<chrono::Utc>)>(
            r#"SELECT id, email, password_hash, role, created_at
               FROM users.accounts
               WHERE id = $1"#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| DomainError::NotFound)?;

        Ok(row.map(|(id, email, password_hash, role, created_at)| Account {
            id,
            email,
            password_hash,
            role,
            created_at,
        }))
    }
}
