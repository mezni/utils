use bornemap_core::{AppError, Session, SessionRepository, UserRepository};
use chrono::Utc;
use uuid::Uuid;

use crate::infrastructure::jwt::JwtService;
use crate::infrastructure::password;

#[derive(Debug)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug)]
pub struct AuthTokens {
    pub access_token: String,
    pub refresh_token: String,
    pub token_type: String,
    pub expires_in: i64,
}

pub struct LoginUseCase<R: UserRepository, S: SessionRepository> {
    user_repo: R,
    session_repo: S,
    jwt_service: JwtService,
    refresh_ttl_seconds: i64,
}

impl<R: UserRepository, S: SessionRepository> LoginUseCase<R, S> {
    pub fn new(
        user_repo: R,
        session_repo: S,
        jwt_service: JwtService,
        refresh_ttl_seconds: i64,
    ) -> Self {
        Self {
            user_repo,
            session_repo,
            jwt_service,
            refresh_ttl_seconds,
        }
    }

    pub async fn execute(&self, req: LoginRequest) -> Result<AuthTokens, AppError> {
        let email = req.email.trim().to_lowercase();

        let user = self
            .user_repo
            .find_by_email(&email)
            .await
            .map_err(AppError::from)?
            .ok_or(AppError::InvalidCredentials)?;

        let valid = password::verify_password(&req.password, &user.password_hash)
            .map_err(|_| AppError::InternalError)?;

        if !valid {
            return Err(AppError::InvalidCredentials);
        }

        let access_token = self
            .jwt_service
            .generate_token(&user.id.to_string(), user.role.as_str())?;

        let (refresh_token, token_hash) = JwtService::generate_refresh_token();

        let now = Utc::now();
        let expires_at = now + chrono::Duration::seconds(self.refresh_ttl_seconds);

        let session = Session {
            id: Uuid::new_v4(),
            user_id: user.id,
            token_hash,
            family_id: Uuid::new_v4(),
            created_at: now,
            expires_at,
            last_used_at: now,
            revoked: false,
            revoked_at: None,
        };

        self.session_repo.create(&session).await?;

        Ok(AuthTokens {
            access_token,
            refresh_token,
            token_type: "Bearer".into(),
            expires_in: self.refresh_ttl_seconds,
        })
    }
}
