use bornemap_core::{AuthError, User, UserRepository, UserRole, UserStatus};
use chrono::Utc;
use uuid::Uuid;

use crate::infrastructure::password;

#[derive(Debug)]
pub struct RegisterRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug)]
pub struct RegisterResponse {
    pub user_id: String,
}

pub struct RegisterUseCase<R: UserRepository> {
    repo: R,
}

impl<R: UserRepository> RegisterUseCase<R> {
    pub fn new(repo: R) -> Self {
        Self { repo }
    }

    pub async fn execute(&self, req: RegisterRequest) -> Result<RegisterResponse, AuthError> {
        let email = req.email.trim().to_lowercase();

        if self.repo.email_exists(&email).await? {
            return Err(AuthError::EmailAlreadyExists);
        }

        let password_hash = password::hash_password(&req.password)
            .map_err(|_| AuthError::InternalError)?;

        let user = User {
            id: Uuid::new_v4(),
            email: email.clone(),
            password_hash,
            role: UserRole::RegisteredDriver,
            status: UserStatus::Active,
            created_at: Utc::now(),
        };

        self.repo.create(&user).await?;

        Ok(RegisterResponse {
            user_id: user.id.to_string(),
        })
    }
}
