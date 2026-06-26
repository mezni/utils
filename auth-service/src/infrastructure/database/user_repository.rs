use sqlx::postgres::PgPool;
use sqlx::PgPoolOptions;
use tracing::{error, info};
use shared_database::Database;

pub struct DatabaseInfrastructure {
    database: Database,
}

impl DatabaseInfrastructure {
    pub async fn new(connection_string: &str) -> Result<Self, sqlx::Error> {
        info!("Initializing database infrastructure...");

        let database = Database::from_connection_string(connection_string).await?;
        let pool = database.get_pool_ref().clone();

        // Configure connection pool
        let configured_pool = PgPoolOptions::new()
            .max_connections(20)
            .acquire_timeout(std::time::Duration::from_secs(30))
            .connect(&pool).await?;

        let db_infra = DatabaseInfrastructure {
            database: Database::from_pool(configured_pool),
        };

        info!("Database infrastructure initialized successfully");

        Ok(db_infra)
    }

    pub fn get_database(&self) -> &Database {
        &self.database
    }
}

pub struct UserRepositoryInfrastructure {
    database: Database,
}

impl UserRepositoryInfrastructure {
    pub fn new(database: Database) -> Self {
        UserRepositoryInfrastructure { database }
    }

    pub async fn find_by_email(&self, email: &str) -> Result<Option<shared_contracts::User>, sqlx::Error> {
        let query = r#"
            SELECT id, email, email_verified, status, created_at, updated_at, deleted_at
            FROM users
            WHERE email = $1
        "#;

        let result = sqlx::query_as::<_, (uuid::Uuid, String, bool, String, DateTime<chrono::Utc>, DateTime<chrono::Utc>, Option<DateTime<chrono::Utc>>)>(
            query
        )
        .bind(email)
        .fetch_optional(&self.database.get_pool())
        .await?;

        Ok(result.map(|(id, email, email_verified, status, created_at, updated_at, deleted_at)| {
            shared_contracts::User {
                id,
                email,
                email_verified,
                status,
                created_at,
                updated_at,
                deleted_at,
            }
        }))
    }

    pub async fn create(&self, user: &shared_contracts::User) -> Result<shared_contracts::User, sqlx::Error> {
        let query = r#"
            INSERT INTO users (id, email, email_verified, status, created_at, updated_at, deleted_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id, email, email_verified, status, created_at, updated_at, deleted_at
        "#;

        let result = sqlx::query_as::<_, (uuid::Uuid, String, bool, String, DateTime<chrono::Utc>, DateTime<chrono::Utc>, Option<DateTime<chrono::Utc>>)>(
            query
        )
        .bind(user.id)
        .bind(&user.email)
        .bind(user.email_verified)
        .bind(&user.status)
        .bind(user.created_at)
        .bind(user.updated_at)
        .bind(user.deleted_at)
        .fetch_one(&self.database.get_pool())
        .await?;

        Ok(shared_contracts::User {
            id: result.0,
            email: result.1,
            email_verified: result.2,
            status: result.3,
            created_at: result.4,
            updated_at: result.5,
            deleted_at: result.6,
        })
    }

    pub async fn update(&self, user_id: uuid::Uuid, updates: shared_contracts::UserUpdates) -> Result<shared_contracts::User, sqlx::Error> {
        let mut query = r#"
            UPDATE users
            SET updated_at = $1
        "#;
        let mut bindings = vec![
            uuid::Uuid::new_v4(), // updated_at parameter
        ];

        if let Some(email) = updates.email {
            query.push_str(", email = $2");
            bindings.push(email);
        }
        if let Some(email_verified) = updates.email_verified {
            query.push_str(", email_verified = $3");
            bindings.push(email_verified);
        }
        if let Some(status) = updates.status {
            query.push_str(", status = $4");
            bindings.push(status);
        }

        query.push_str(" WHERE id = $");
        query.push_str(&(bindings.len() + 1).to_string());
        bindings.push(user_id);

        query.push_str(" RETURNING id, email, email_verified, status, created_at, updated_at, deleted_at");

        let result = sqlx::query_as::<_, (uuid::Uuid, String, bool, String, DateTime<chrono::Utc>, DateTime<chrono::Utc>, Option<DateTime<chrono::Utc>>)>(
            query
        )
        .bind(bindings[0])
        .bind(bindings.get(1))
        .bind(bindings.get(2))
        .bind(bindings.get(3))
        .bind(bindings.get(4))
        .fetch_one(&self.database.get_pool())
        .await?;

        Ok(shared_contracts::User {
            id: result.0,
            email: result.1,
            email_verified: result.2,
            status: result.3,
            created_at: result.4,
            updated_at: result.5,
            deleted_at: result.6,
        })
    }

    pub async fn find_by_id(&self, user_id: uuid::Uuid) -> Result<Option<shared_contracts::User>, sqlx::Error> {
        let query = r#"
            SELECT id, email, email_verified, status, created_at, updated_at, deleted_at
            FROM users
            WHERE id = $1
        "#;

        let result = sqlx::query_as::<_, (uuid::Uuid, String, bool, String, DateTime<chrono::Utc>, DateTime<chrono::Utc>, Option<DateTime<chrono::Utc>>)>(
            query
        )
        .bind(user_id)
        .fetch_optional(&self.database.get_pool())
        .await?;

        Ok(result.map(|(id, email, email_verified, status, created_at, updated_at, deleted_at)| {
            shared_contracts::User {
                id,
                email,
                email_verified,
                status,
                created_at,
                updated_at,
                deleted_at,
            }
        }))
    }

    pub async fn delete(&self, user_id: uuid::Uuid) -> Result<(), sqlx::Error> {
        let query = r#"
            UPDATE users
            SET deleted_at = $1
            WHERE id = $2
        "#;

        sqlx::query(query)
            .bind(Utc::now())
            .bind(user_id)
            .execute(&self.database.get_pool())
            .await?;

        Ok(())
    }
}

pub struct RefreshTokenRepositoryInfrastructure {
    database: Database,
}

impl RefreshTokenRepositoryInfrastructure {
    pub fn new(database: Database) -> Self {
        RefreshTokenRepositoryInfrastructure { database }
    }

    pub async fn create(&self, token: &shared_contracts::RefreshToken) -> Result<shared_contracts::RefreshToken, sqlx::Error> {
        let query = r#"
            INSERT INTO refresh_tokens (id, user_id, jti, token_hash, expires_at, revoked_at, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id, user_id, jti, token_hash, expires_at, revoked_at, created_at
        "#;

        let result = sqlx::query_as::<_, (uuid::Uuid, uuid::Uuid, uuid::Uuid, String, DateTime<chrono::Utc>, Option<DateTime<chrono::Utc>>, DateTime<chrono::Utc>)>(
            query
        )
        .bind(token.id)
        .bind(token.user_id)
        .bind(token.jti)
        .bind(&token.token_hash)
        .bind(token.expires_at)
        .bind(token.revoked_at)
        .bind(token.created_at)
        .fetch_one(&self.database.get_pool())
        .await?;

        Ok(shared_contracts::RefreshToken {
            id: result.0,
            user_id: result.1,
            jti: result.2,
            token_hash: result.3,
            expires_at: result.4,
            revoked_at: result.5,
            created_at: result.6,
        })
    }

    pub async fn find_by_jti(&self, jti: uuid::Uuid) -> Result<Option<shared_contracts::RefreshToken>, sqlx::Error> {
        let query = r#"
            SELECT id, user_id, jti, token_hash, expires_at, revoked_at, created_at
            FROM refresh_tokens
            WHERE jti = $1
        "#;

        let result = sqlx::query_as::<_, (uuid::Uuid, uuid::Uuid, uuid::Uuid, String, DateTime<chrono::Utc>, Option<DateTime<chrono::Utc>>, DateTime<chrono::Utc>)>(
            query
        )
        .bind(jti)
        .fetch_optional(&self.database.get_pool())
        .await?;

        Ok(result.map(|(id, user_id, jti, token_hash, expires_at, revoked_at, created_at)| {
            shared_contracts::RefreshToken {
                id,
                user_id,
                jti,
                token_hash,
                expires_at,
                revoked_at,
                created_at,
            }
        }))
    }

    pub async fn find_by_user_id(&self, user_id: uuid::Uuid) -> Result<Vec<shared_contracts::RefreshToken>, sqlx::Error> {
        let query = r#"
            SELECT id, user_id, jti, token_hash, expires_at, revoked_at, created_at
            FROM refresh_tokens
            WHERE user_id = $1
        "#;

        let result = sqlx::query_as::<_, (uuid::Uuid, uuid::Uuid, uuid::Uuid, String, DateTime<chrono::Utc>, Option<DateTime<chrono::Utc>>, DateTime<chrono::Utc>)>(
            query
        )
        .bind(user_id)
        .fetch_all(&self.database.get_pool())
        .await?;

        Ok(result.into_iter().map(|(id, user_id, jti, token_hash, expires_at, revoked_at, created_at)| {
            shared_contracts::RefreshToken {
                id,
                user_id,
                jti,
                token_hash,
                expires_at,
                revoked_at,
                created_at,
            }
        }).collect())
    }

    pub async fn revoke(&self, jti: uuid::Uuid) -> Result<(), sqlx::Error> {
        let query = r#"
            UPDATE refresh_tokens
            SET revoked_at = $1
            WHERE jti = $2
        "#;

        sqlx::query(query)
            .bind(Utc::now())
            .bind(jti)
            .execute(&self.database.get_pool())
            .await?;

        Ok(())
    }

    pub async fn revoke_all_by_user_id(&self, user_id: uuid::Uuid) -> Result<(), sqlx::Error> {
        let query = r#"
            UPDATE refresh_tokens
            SET revoked_at = $1
            WHERE user_id = $2
        "#;

        sqlx::query(query)
            .bind(Utc::now())
            .bind(user_id)
            .execute(&self.database.get_pool())
            .await?;

        Ok(())
    }

    pub async fn delete_expired(&self) -> Result<u64, sqlx::Error> {
        let query = r#"
            DELETE FROM refresh_tokens
            WHERE expires_at < $1
        "#;

        let result = sqlx::query(query)
            .bind(Utc::now())
            .execute(&self.database.get_pool())
            .await?;

        Ok(result.rows_affected())
    }
}