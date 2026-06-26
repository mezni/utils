use shared_contracts::RefreshToken;
use uuid::Uuid;
use sqlx::Error;

#[async_trait::async_trait]
pub trait RefreshTokenRepository: Send + Sync {
    async fn create(&self, token: &RefreshToken) -> Result<RefreshToken, Error>;
    async fn find_by_jti(&self, jti: Uuid) -> Result<Option<RefreshToken>, Error>;
    async fn find_by_user_id(&self, user_id: Uuid) -> Result<Vec<RefreshToken>, Error>;
    async fn revoke(&self, jti: Uuid) -> Result<(), Error>;
    async fn revoke_all_by_user_id(&self, user_id: Uuid) -> Result<(), Error>;
    async fn delete_expired(&self) -> Result<u64, Error>;
}