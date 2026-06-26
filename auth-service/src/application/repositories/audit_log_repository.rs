use shared_contracts::AuditLog;
use uuid::Uuid;
use sqlx::Error;

#[async_trait::async_trait]
pub trait AuditLogRepository: Send + Sync {
    async fn create(&self, log: &AuditLog) -> Result<AuditLog, Error>;
    async fn find_by_user_id(&self, user_id: Uuid, limit: i64) -> Result<Vec<AuditLog>, Error>;
    async fn find_recent(&self, limit: i64) -> Result<Vec<AuditLog>, Error>;
    async fn find_by_ip_address(&self, ip_address: &str, limit: i64) -> Result<Vec<AuditLog>, Error>;
}