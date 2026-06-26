use sqlx::{Error, PgPool};
use tracing::{error, info};
use shared_contracts::AuditLog;

pub struct AuditLogRepositoryInfrastructure {
    database: PgPool,
}

impl AuditLogRepositoryInfrastructure {
    pub fn new(database: PgPool) -> Self {
        AuditLogRepositoryInfrastructure { database }
    }

    pub async fn create(&self, log: &AuditLog) -> Result<AuditLog, Error> {
        let query = r#"
            INSERT INTO login_audit_log (id, user_id, email, ip_address, user_agent, success, failure_reason, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id, user_id, email, ip_address, user_agent, success, failure_reason, created_at
        "#;

        let result = sqlx::query_as::<_, (uuid::Uuid, Option<uuid::Uuid>, String, Option<String>, Option<String>, bool, Option<String>, DateTime<chrono::Utc>)>(
            query
        )
        .bind(log.id)
        .bind(log.user_id)
        .bind(&log.email)
        .bind(&log.ip_address)
        .bind(&log.user_agent)
        .bind(log.success)
        .bind(&log.failure_reason)
        .bind(log.created_at)
        .fetch_one(&self.database)
        .await?;

        Ok(AuditLog {
            id: result.0,
            user_id: result.1,
            email: result.2,
            ip_address: result.3,
            user_agent: result.4,
            success: result.5,
            failure_reason: result.6,
            created_at: result.7,
        })
    }

    pub async fn find_by_user_id(&self, user_id: uuid::Uuid, limit: i64) -> Result<Vec<AuditLog>, Error> {
        let query = r#"
            SELECT id, user_id, email, ip_address, user_agent, success, failure_reason, created_at
            FROM login_audit_log
            WHERE user_id = $1
            ORDER BY created_at DESC
            LIMIT $2
        "#;

        let result = sqlx::query_as::<_, (uuid::Uuid, Option<uuid::Uuid>, String, Option<String>, Option<String>, bool, Option<String>, DateTime<chrono::Utc>)>(
            query
        )
        .bind(user_id)
        .bind(limit)
        .fetch_all(&self.database)
        .await?;

        Ok(result.into_iter().map(|(id, user_id, email, ip_address, user_agent, success, failure_reason, created_at)| {
            AuditLog {
                id,
                user_id,
                email,
                ip_address,
                user_agent,
                success,
                failure_reason,
                created_at,
            }
        }).collect())
    }

    pub async fn find_recent(&self, limit: i64) -> Result<Vec<AuditLog>, Error> {
        let query = r#"
            SELECT id, user_id, email, ip_address, user_agent, success, failure_reason, created_at
            FROM login_audit_log
            ORDER BY created_at DESC
            LIMIT $1
        "#;

        let result = sqlx::query_as::<_, (uuid::Uuid, Option<uuid::Uuid>, String, Option<String>, Option<String>, bool, Option<String>, DateTime<chrono::Utc>)>(
            query
        )
        .bind(limit)
        .fetch_all(&self.database)
        .await?;

        Ok(result.into_iter().map(|(id, user_id, email, ip_address, user_agent, success, failure_reason, created_at)| {
            AuditLog {
                id,
                user_id,
                email,
                ip_address,
                user_agent,
                success,
                failure_reason,
                created_at,
            }
        }).collect())
    }

    pub async fn find_by_ip_address(&self, ip_address: &str, limit: i64) -> Result<Vec<AuditLog>, Error> {
        let query = r#"
            SELECT id, user_id, email, ip_address, user_agent, success, failure_reason, created_at
            FROM login_audit_log
            WHERE ip_address = $1
            ORDER BY created_at DESC
            LIMIT $2
        "#;

        let result = sqlx::query_as::<_, (uuid::Uuid, Option<uuid::Uuid>, String, Option<String>, Option<String>, bool, Option<String>, DateTime<chrono::Utc>)>(
            query
        )
        .bind(ip_address)
        .bind(limit)
        .fetch_all(&self.database)
        .await?;

        Ok(result.into_iter().map(|(id, user_id, email, ip_address, user_agent, success, failure_reason, created_at)| {
            AuditLog {
                id,
                user_id,
                email,
                ip_address,
                user_agent,
                success,
                failure_reason,
                created_at,
            }
        }).collect())
    }
}