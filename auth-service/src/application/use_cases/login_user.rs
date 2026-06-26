use chrono::{DateTime, Utc};
use shared_contracts::{User, UserWithoutSensitive, AuthError, ValidationError, TokenResponse, UserPassword};
use crate::domain::{RefreshToken, RefreshTokenString, User as DomainUser};
use crate::application::repositories::{UserRepository, RefreshTokenRepository, AuditLogRepository};
use crate::infrastructure::jwt::JwtService;
use crate::infrastructure::cache::Cache;
use crate::domain::value_objects::PasswordHash;
use crate::domain::services::{PasswordService, TokenPolicyService};
use tracing::{error, info, warn};

pub struct LoginUserUseCase {
    user_repository: Box<dyn UserRepository>,
    refresh_token_repository: Box<dyn RefreshTokenRepository>,
    audit_log_repository: Box<dyn AuditLogRepository>,
    jwt_service: JwtService,
    cache: Cache,
    password_pepper: String,
}

impl LoginUserUseCase {
    pub fn new(
        user_repository: Box<dyn UserRepository>,
        refresh_token_repository: Box<dyn RefreshTokenRepository>,
        audit_log_repository: Box<dyn AuditLogRepository>,
        jwt_service: JwtService,
        cache: Cache,
        password_pepper: String,
    ) -> Self {
        LoginUserUseCase {
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
        email: String,
        password: String,
        ip_address: Option<String>,
        user_agent: Option<String>,
    ) -> Result<TokenResponse, AuthError> {
        info!("Login attempt for: {}", email);

        // Validate email format
        let email_obj = Email::new(email.clone()).map_err(|e| {
            error!("Email validation failed: {}", e);
            AuthError::new("VALIDATION_ERROR", e).with_details(vec!["email".to_string()])
        })?;

        // Find user
        let user = self.user_repository.find_by_email(email_obj.as_str()).await.map_err(|e| {
            error!("Database error finding user: {}", e);
            AuthError::new("INTERNAL_ERROR", "Failed to find user".to_string())
        })?;

        let user = user.ok_or_else(|| {
            warn!("Login failed: user not found for email {}", email);
            AuthError::new("INVALID_CREDENTIALS", "Invalid email or password".to_string())
        })?;

        // Get user password
        let password_hash = self.get_user_password_hash(&user).await.map_err(|e| {
            error!("Failed to get user password: {}", e);
            AuthError::new("INTERNAL_ERROR", "Failed to verify password".to_string())
        })?;

        // Verify password
        PasswordService::verify(&password, &password_hash, &self.password_pepper)
            .map_err(|e| {
                error!("Password verification failed: {}", e);
                AuthError::new("INVALID_CREDENTIALS", "Invalid email or password".to_string())
            })?;

        // Check if user is active
        if !user.is_active() {
            warn!("Login failed: user account is inactive: {}", email);
            return Err(AuthError::new("INVALID_CREDENTIALS", "Account is inactive or deleted".to_string()));
        }

        // Check if user is verified
        if !user.email_verified {
            warn!("Login failed: user email not verified: {}", email);
            return Err(AuthError::new("EMAIL_NOT_VERIFIED", "Please verify your email before logging in".to_string()));
        }

        // Generate tokens
        let claims = self.jwt_service.generate_claims(&user, 5)?;
        let access_token = self.jwt_service.sign(&claims)?;
        let refresh_token_str = RefreshTokenString {
            token: PasswordService::generate_refresh_token(),
        };
        let refresh_token_hash = self.jwt_service.hash_token(&refresh_token_str.token);
        let refresh_expires_at = TokenPolicyService::generate_refresh_token_expiration();

        // Create refresh token record
        let jti = TokenPolicyService::generate_jti();
        let refresh_token = RefreshToken::new(user.id, jti, refresh_expires_at);
        refresh_token.set_token_hash(refresh_token_hash);

        let saved_refresh_token = self.refresh_token_repository.create(&refresh_token).await.map_err(|e| {
            error!("Failed to create refresh token: {}", e);
            AuthError::new("INTERNAL_ERROR", "Failed to create refresh token".to_string())
        })?;

        // Add refresh token to Redis blacklist
        let token_expires_in = TokenPolicyService::remaining_lifetime(refresh_expires_at);
        let redis_key = format!("jti_blacklist:{}", saved_refresh_token.jti);
        self.cache.set_with_ttl(&redis_key, &saved_refresh_token.token, token_expires_in as u64).await.map_err(|e| {
            error!("Failed to add refresh token to Redis blacklist: {}", e);
            AuthError::new("INTERNAL_ERROR", "Failed to process login".to_string())
        })?;

        // Log successful login
        self.audit_log_repository.create(&shared_contracts::AuditLog {
            id: uuid::Uuid::new_v4(),
            user_id: Some(user.id),
            email: email.clone(),
            ip_address,
            user_agent,
            success: true,
            failure_reason: None,
            created_at: Utc::now(),
        }).await.map_err(|e| {
            error!("Failed to log successful login: {}", e);
            AuthError::new("INTERNAL_ERROR", "Failed to process login".to_string())
        })?;

        info!("Login successful for user: {}", email);

        Ok(TokenResponse {
            access_token,
            refresh_token: refresh_token_str.token,
            access_token_expires_in: 300,
            refresh_token_expires_in: token_expires_in,
        })
    }

    async fn get_user_password_hash(&self, user: &DomainUser) -> Result<PasswordHash, sqlx::Error> {
        let query = r#"
            SELECT password_hash
            FROM user_passwords
            WHERE user_id = $1
            ORDER BY updated_at DESC
            LIMIT 1
        "#;

        let row = sqlx::query(query)
            .bind(user.id)
            .fetch_optional(&self.user_repository.get_pool())
            .await?;

        row.ok_or_else(|| sqlx::Error::RowNotFound)
            .map(|row| {
                let password_hash = row.get::<String, _>("password_hash");
                let algorithm = "argon2id".to_string();
                let cost = 12;
                PasswordHash::new(password_hash, algorithm, cost)
            })
    }
}