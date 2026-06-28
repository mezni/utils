use async_trait::async_trait;
use uuid::Uuid;

use super::account::Account;
use super::error::DomainError;

#[async_trait]
pub trait AccountRepository: Send + Sync {
    async fn create(&self, account: &Account) -> Result<(), DomainError>;
    async fn find_by_email(&self, email: &str) -> Result<Option<Account>, DomainError>;
    async fn find_by_id(&self, id: Uuid) -> Result<Option<Account>, DomainError>;
}
