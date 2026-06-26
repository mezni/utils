use chrono::{DateTime, Utc};
use shared_contracts::{AuthError, TokenResponse, RefreshTokenString};
use crate::domain::{RefreshToken, User as DomainUser, RefreshTokenString as DomainRefreshToken};
use crate::application::repositories::{UserRepository, RefreshTokenRepository, AuditLogRepository};
use crate::infrastructure::jwt::JwtService;
use crate::infrastructure::cache::Cache;
use crate::domain::value_objects::PasswordHash;
use crate::domain::services::TokenPolicyService;
use tracing::{error, info, warn};

pub struct RefreshTokenUseCase {
    user_repository: Box<dyn UserRepository>,
    refresh_token_repository: Box<dyn RefreshTokenRepository>,
    audit_log_repository: Box<dyn AuditLogRepository>,
    jwt_service: JwtService,
    cache: Cache,
    password_pepper: String,
}

impl RefreshTokenUseCase {
    pub fn new(
        user_repository: Box<dyn UserRepository>,
        refresh_token_repository: Box<dyn RefreshTokenRepository>,
        audit_log_repository: Box<dyn AuditLogRepository>,
        jwt_service: JwtService,
        cache: Cache,
        password_pepper: String,
    ) -> Self {
        RefreshTokenUseCase {
            user_repository,
            refresh_token_repository,
            audit_log_repository,
            jwt_service,
            cache,
            password_pepper,
        }
    }

    pub async fn execute(
        &self,
        refresh_token_str: String,
        ip_address: Option<String>,
        user_agent: Option<String>,
    ) -> Result<TokenResponse, AuthError> {
        info!("Refresh token request received");

        // Validate token hash
        let token_hash = self.jwt_service.hash_token(&refresh_token_str);

        // Find token in database
        let token = self.refresh_token_repository.find_by_jti(uuid::Uuid::new_v4()).await.map_err(|e| {
            error!("Database error finding refresh token: {}", e);
            AuthError::new("INTERNAL_ERROR", "Failed to verify refresh token".to_string())
        })?;

        let token = token.ok_or_else(|| {
            warn!("Refresh token invalid or not found");
            AuthError::new("INVALID_REFRESH_TOKEN", "Refresh token invalid or not found".to_string())
        })?;

        // Check if token is revoked
        if token.is_revoked() {
            warn!("Refresh token revoked: {}", token.jti);
            return Err(AuthError::new("REFRESH_TOKEN_REVOKED", "Refresh token has been revoked".to_string()));
        }

        // Check if token is expired
        if token.is_expired() {
            warn!("Refresh token expired: {}", token.jti);
            return Err(AuthError::new("REFRESH_TOKEN_EXPIRED", "Refresh token has expired".to_string()));
        }

        // Check token hash
        if token.token_hash != token_hash {
            warn!("Refresh token hash mismatch: {}", token.jti);
            return Err(AuthError::new("INVALID_REFRESH_TOKEN", "Refresh token invalid or not found".to_string()));
        }

        // Check for token reuse (old token ID matches new one)
        let old_jti = uuid::Uuid::new_v4(); // This would be passed from the JWT claims
        if TokenPolicyService::check_token_reuse(old_jti, token.jti) {
            error!("CRITICAL: Refresh token reuse detected - security breach attempt");
            
            // Revoke all tokens for this user
            let _ = self.refresh_token_repository.revoke_all_by_user_id(token.user_id).await;
            
            return Err(AuthError::new("TOKEN_REUSE", "Refresh token reuse detected. Token revoked immediately.".to_string()));
        }

        // Get user
        let user = self.user_repository.find_by_id(token.user_id).await.map_err(|e| {
            error!("Database error finding user: {}", e);
            AuthError::new("INTERNAL_ERROR", "Failed to verify refresh token".to_string())
        })?;

        let user = user.ok_or_else(|| {
            warn!("User not found for refresh token: {}", token.user_id);
            AuthError::new("INVALID_REFRESH_TOKEN", "User not found for refresh token".to_string())
        })?;

        // Check if user is active
        if !user.is_active() {
            warn!("User account is inactive for refresh token: {}", token.user_id);
            return Err(AuthError::new("INVALID_REFRESH_TOKEN", "User account is inactive".to_string()));
        }

        // Check if user is verified
        if !user.email_verified {
            warn!("User email not verified for refresh token: {}", token.user_id);
            return Err(AuthError::new("EMAIL_NOT_VERIFIED", "Please verify your email before logging in".to_string()));
        }

        // Revoke old token
        self.refresh_token_repository.revoke(token.jti).await.map_err(|e| {
            error!("Failed to revoke refresh token: {}", e);
            AuthError::new("INTERNAL_ERROR", "Failed to process refresh token".to_string())
        })?;

        // Remove old token from Redis blacklist
        let redis_key = format!("jti_blacklist:{}", token.jti);
        self.cache.del(&redis_key).await.map_err(|e| {
            error!("Failed to remove old token from Redis blacklist: {}", e);
            AuthError::new("INTERNAL_ERROR", "Failed to process refresh token".to_string())
        })?;

        // Generate new tokens
        let claims = self.jwt_service.generate_claims(&user, 5)?;
        let access_token = self.jwt_service.sign(&claims)?;
        let new_refresh_token_str = RefreshTokenString {
            token: PasswordService::generate_refresh_token(),
        };
        let new_refresh_token_hash = self.jwt_service.hash_token(&new_refresh_token_str.token);
        let refresh_expires_at = TokenPolicyService::generate_refresh_token_expiration();

        // Create new refresh token record
        let new_jti = TokenPolicyService::generate_jti();
        let new_refresh_token = RefreshToken::new(user.id, new_jti, refresh_expires_at);
        new_refresh_token.set_token_hash(new_refresh_token_hash);

        let saved_refresh_token = self.refresh_token_repository.create(&new_refresh_token).await.map_err(|e| {
            error!("Failed to create new refresh token: {}", e);
            AuthError::new("INTERNAL_ERROR", "Failed to process refresh token".to_string())
        })?;

        // Add new token to Redis blacklist
        let token_expires_in = TokenPolicyService::remaining_lifetime(refresh_expires_at);
        let redis_key = format!("jti_blacklist:{}", saved_refresh_token.jti);
        self.cache.set_with_ttl(&redis_key, &saved_refresh_token.token, token_expires_in as u64).await.map_err(|e| {
            error!("Failed to add new refresh token to Redis blacklist: {}", e);
            AuthError::new("INTERNAL_ERROR", "Failed to process refresh token".to_string())
        })?;

        // Log refresh token request
        self.audit_log_repository.create(&shared_contracts::AuditLog {
            id: uuid::Uuid::new_v4(),
            user_id: Some(user.id),
            email: user.email,
            ip_address,
            user_agent,
            success: true,
            failure_reason: None,
            created_at: Utc::now(),
        }).await.map_err(|e| {
            error!("Failed to log refresh token request: {}", e);
            AuthError::new("INTERNAL_ERROR", "Failed to process refresh token".to_string())
        })?;

        info!("Refresh token successfully rotated for user: {}", user.email);

        Ok(TokenResponse {
            access_token,
            refresh_token: new_refresh_token_str.token,
            access_token_expires_in: 300,
            refresh_token_expires_in: token_expires_in,
        })
    }
}