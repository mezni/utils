use bornemap_core::{AuthError, User, UserRepository, UserRole, UserStatus};
use chrono::Utc;
use uuid::Uuid;

use crate::infrastructure::jwt::JwtService;
use crate::infrastructure::password::PasswordService;

#[derive(Debug)]
pub struct RegisterRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug)]
pub struct AuthResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: i64,
}

pub struct RegisterUseCase<R: UserRepository> {
    repo: R,
    jwt_service: JwtService,
}

impl<R: UserRepository> RegisterUseCase<R> {
    pub fn new(repo: R, jwt_service: JwtService) -> Self {
        Self { repo, jwt_service }
    }

    pub async fn execute(&self, req: RegisterRequest) -> Result<AuthResponse, AuthError> {
        let email = req.email.trim().to_lowercase();

        if !email.contains('@') || !email.contains('.') {
            return Err(AuthError::ValidationError("Invalid email format".into()));
        }

        if req.password.len() < 8 {
            return Err(AuthError::ValidationError(
                "Password must be at least 8 characters".into(),
            ));
        }

        if req.password.len() > 128 {
            return Err(AuthError::ValidationError(
                "Password must be at most 128 characters".into(),
            ));
        }

        if self.repo.email_exists(&email).await? {
            return Err(AuthError::EmailAlreadyExists);
        }

        let password_hash = PasswordService::hash(&req.password)?;

        let user = User {
            id: Uuid::new_v4(),
            email: email.clone(),
            password_hash,
            role: UserRole::RegisteredDriver,
            status: UserStatus::Active,
            created_at: Utc::now(),
        };

        self.repo.create(&user).await?;

        let access_token = self
            .jwt_service
            .generate_token(&user.id.to_string(), user.role.as_str())
            .map_err(|_| AuthError::InternalError)?;

        Ok(AuthResponse {
            access_token,
            token_type: "Bearer".into(),
            expires_in: 86400,
        })
    }
}
