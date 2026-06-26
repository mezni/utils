use bornemap_core::{AuthError, UserRepository};

use crate::application::register::AuthResponse;
use crate::infrastructure::jwt::JwtService;
use crate::infrastructure::password::PasswordService;

#[derive(Debug)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

pub struct LoginUseCase<R: UserRepository> {
    repo: R,
    jwt_service: JwtService,
}

impl<R: UserRepository> LoginUseCase<R> {
    pub fn new(repo: R, jwt_service: JwtService) -> Self {
        Self { repo, jwt_service }
    }

    pub async fn execute(&self, req: LoginRequest) -> Result<AuthResponse, AuthError> {
        let email = req.email.trim().to_lowercase();

        let user = self
            .repo
            .find_by_email(&email)
            .await?
            .ok_or(AuthError::InvalidCredentials)?;

        let valid = PasswordService::verify(&req.password, &user.password_hash)?;

        if !valid {
            return Err(AuthError::InvalidCredentials);
        }

        let access_token = self
            .jwt_service
            .generate_token(&user.id.to_string(), user.role.as_str())?;

        let expires_in = 86400;

        Ok(AuthResponse {
            access_token,
            token_type: "Bearer".into(),
            expires_in,
        })
    }
}
