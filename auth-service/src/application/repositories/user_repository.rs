use sqlx::Error;
use shared_contracts::{User, UserWithoutSensitive};
use uuid::Uuid;
use async_trait::async_trait;

#[async_trait::async_trait]
pub trait UserRepository: Send + Sync {
    async fn find_by_email(&self, email: &str) -> Result<Option<User>, Error>;
    async fn create(&self, user: &User) -> Result<User, Error>;
    async fn update(&self, user_id: Uuid, updates: UserUpdates) -> Result<User, Error>;
    async fn find_by_id(&self, user_id: Uuid) -> Result<Option<User>, Error>;
    async fn delete(&self, user_id: Uuid) -> Result<(), Error>;
}

#[derive(Debug, Clone)]
pub struct UserUpdates {
    pub email: Option<String>,
    pub email_verified: Option<bool>,
    pub status: Option<String>,
}

impl UserUpdates {
    pub fn empty() -> Self {
        UserUpdates {
            email: None,
            email_verified: None,
            status: None,
        }
    }
}