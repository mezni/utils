use chrono::{DateTime, Utc};
use shared_contracts::{AuthError, LogoutResponse};
use crate::domain::{User as DomainUser, RefreshToken};
use crate::application::repositories::{UserRepository, RefreshTokenRepository, AuditLogRepository};
use crate::infrastructure::jwt::JwtService;
use crate::infrastructure::cache::Cache;
use tracing::{error, info, warn};

pub struct LogoutUseCase {
    user_repository: Box<dyn UserRepository>,
    refresh_token_repository: Box<dyn RefreshTokenRepository>,
    audit_log_repository: Box<dyn AuditLogRepository>,
    jwt_service: JwtService,
    cache: Cache,
}

impl LogoutUseCase {
    pub fn new(
        user_repository: Box<dyn UserRepository>,
        refresh_token_repository: Box<dyn RefreshTokenRepository>,
        audit_log_repository: Box<dyn AuditLogRepository>,
        jwt_service: JwtService,
        cache: Cache,
    ) -> Self {
        LogoutUseCase {
            user_repository,
            refresh_token_repository,
            audit_log_repository,
            jwt_service,
            cache,
        }
    }

    pub async fn execute(
        &self,
        user_id: uuid::Uuid,
        ip_address: Option<String>,
        user_agent: Option<String>,
    ) -> Result<LogoutResponse, AuthError> {
        info!("Processing logout for user: {}", user_id);

        // Revoke all refresh tokens for this user
        self.refresh_token_repository.revoke_all_by_user_id(user_id).await.map_err(|e| {
            error!("Failed to revoke refresh tokens: {}", e);
            AuthError::new("INTERNAL_ERROR", "Failed to process logout".to_string())
        })?;

        // Clear all tokens from Redis blacklist for this user
        let all_user_tokens = self.refresh_token_repository.find_by_user_id(user_id).await.map_err(|e| {
            error!("Failed to find user tokens: {}", e);
            AuthError::new("INTERNAL_ERROR", "Failed to process logout".to_string())
        })?;

        for token in all_user_tokens {
            let redis_key = format!("jti_blacklist:{}", token.jti);
            self.cache.del(&redis_key).await.map_err(|e| {
                error!("Failed to remove token from Redis blacklist: {}", e);
                AuthError::new("INTERNAL_ERROR", "Failed to process logout".to_string())
            })?;
        }

        // Log logout
        self.audit_log_repository.create(&shared_contracts::AuditLog {
            id: uuid::Uuid::new_v4(),
            user_id: Some(user_id),
            email: "user".to_string(), // We don't have the email in the context
            ip_address,
            user_agent,
            success: true,
            failure_reason: None,
            created_at: Utc::now(),
        }).await.map_err(|e| {
            error!("Failed to log logout: {}", e);
            AuthError::new("INTERNAL_ERROR", "Failed to process logout".to_string())
        })?;

        info!("User logged out successfully: {}", user_id);

        Ok(LogoutResponse {
            success: true,
            message: "Successfully logged out".to_string(),
        })
    }
}