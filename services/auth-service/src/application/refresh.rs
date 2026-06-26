use bornemap_core::{AppError, SessionRepository, UserRepository};
use chrono::Utc;
use uuid::Uuid;

use crate::application::login::AuthTokens;
use crate::infrastructure::jwt::JwtService;

pub struct RefreshRequest {
    pub refresh_token: String,
}

pub struct RefreshUseCase<R: UserRepository, S: SessionRepository> {
    user_repo: R,
    session_repo: S,
    jwt_service: JwtService,
    refresh_ttl_seconds: i64,
}

impl<R: UserRepository, S: SessionRepository> RefreshUseCase<R, S> {
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

    pub async fn execute(&self, req: RefreshRequest) -> Result<AuthTokens, AppError> {
        let token_hash = bornemap_auth::hash_refresh_token(&req.refresh_token);

        let session = self
            .session_repo
            .find_by_token_hash(&token_hash)
            .await?
            .ok_or(AppError::InvalidSession)?;

        if session.revoked {
            self.session_repo
                .revoke_family(session.family_id)
                .await?;
            return Err(AppError::InvalidSession);
        }

        if Utc::now() > session.expires_at {
            self.session_repo
                .revoke_session(session.id)
                .await?;
            return Err(AppError::ExpiredSession);
        }

        self.session_repo
            .revoke_session(session.id)
            .await?;

        let user = self
            .user_repo
            .find_by_id(session.user_id)
            .await
            .map_err(AppError::from)?
            .ok_or(AppError::InvalidSession)?;

        let access_token = self
            .jwt_service
            .generate_token(&user.id.to_string(), user.role.as_str())?;

        let (new_refresh_token, new_token_hash) = JwtService::generate_refresh_token();

        let now = Utc::now();
        let expires_at = now + chrono::Duration::seconds(self.refresh_ttl_seconds);

        let new_session = bornemap_core::Session {
            id: Uuid::new_v4(),
            user_id: user.id,
            token_hash: new_token_hash,
            family_id: session.family_id,
            created_at: now,
            expires_at,
            last_used_at: now,
            revoked: false,
            revoked_at: None,
        };

        self.session_repo.create(&new_session).await?;

        Ok(AuthTokens {
            access_token,
            refresh_token: new_refresh_token,
            token_type: "Bearer".into(),
            expires_in: self.refresh_ttl_seconds,
        })
    }
}
