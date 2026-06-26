use chrono::{DateTime, Utc};
use shared_contracts::{User, UserWithoutSensitive, AuthError, ValidationError, RegisterResponse, TokenResponse};
use crate::domain::{Email, User as DomainUser};
use crate::application::repositories::{UserRepository, RefreshTokenRepository, AuditLogRepository};
use crate::infrastructure::jwt::JwtService;
use crate::infrastructure::cache::Cache;
use crate::domain::value_objects::PasswordHash;
use crate::domain::services::PasswordService;
use tracing::{error, info, warn};

pub struct RegisterUserUseCase {
    user_repository: Box<dyn UserRepository>,
    refresh_token_repository: Box<dyn RefreshTokenRepository>,
    audit_log_repository: Box<dyn AuditLogRepository>,
    jwt_service: JwtService,
    password_pepper: String,
}

impl RegisterUserUseCase {
    pub fn new(
        user_repository: Box<dyn UserRepository>,
        refresh_token_repository: Box<dyn RefreshTokenRepository>,
        audit_log_repository: Box<dyn AuditLogRepository>,
        jwt_service: JwtService,
        password_pepper: String,
    ) -> Self {
        RegisterUserUseCase {
            user_repository,
            refresh_token_repository,
            audit_log_repository,
            jwt_service,
            password_pepper,
        }
    }

    pub async fn execute(
        &self,
        email: String,
        password: String,
        ip_address: Option<String>,
        user_agent: Option<String>,
    ) -> Result<RegisterResponse, AuthError> {
        info!("Registering new user: {}", email);

        // Validate password strength
        PasswordService::validate_password_strength(&password).map_err(|e| {
            error!("Password validation failed: {}", e);
            AuthError::new("PASSWORD_ERROR", e).with_details(vec!["password".to_string()])
        })?;

        // Validate email format
        let email_obj = Email::new(email.clone()).map_err(|e| {
            error!("Email validation failed: {}", e);
            AuthError::new("VALIDATION_ERROR", e).with_details(vec!["email".to_string()])
        })?;

        // Check if user already exists
        let existing_user = self.user_repository.find_by_email(&email_obj.as_str()).await.map_err(|e| {
            error!("Database error checking user existence: {}", e);
            AuthError::new("INTERNAL_ERROR", "Failed to check user existence".to_string())
        })?;

        if existing_user.is_some() {
            return Err(AuthError::new("USER_EXISTS", format!("User with email {} already exists", email))
                .with_details(vec!["email".to_string()]));
        }

        // Hash password
        let password_hash = PasswordService::hash(&password, &self.password_pepper)
            .map_err(|e| {
                error!("Password hashing failed: {}", e);
                AuthError::new("INTERNAL_ERROR", "Failed to hash password".to_string())
            })?;

        // Create user
        let user_id = uuid::Uuid::new_v4();
        let now = Utc::now();
        let user = DomainUser::new(user_id, email_obj.as_str().to_string(), false, "active".to_string());

        let saved_user = self.user_repository.create(&user).await.map_err(|e| {
            error!("Failed to create user: {}", e);
            AuthError::new("INTERNAL_ERROR", "Failed to create user".to_string())
        })?;

        // Create user_passwords entry
        self.create_user_password(&saved_user, &password_hash).await.map_err(|e| {
            error!("Failed to create user password: {}", e);
            AuthError::new("INTERNAL_ERROR", "Failed to create user password".to_string())
        })?;

        // Log the registration attempt
        self.audit_log_repository.create(&shared_contracts::AuditLog {
            id: uuid::Uuid::new_v4(),
            user_id: Some(user_id),
            email: email.clone(),
            ip_address,
            user_agent,
            success: true,
            failure_reason: None,
            created_at: now,
        }).await.map_err(|e| {
            error!("Failed to log registration: {}", e);
            AuthError::new("INTERNAL_ERROR", "Failed to log registration".to_string())
        })?;

        info!("User registered successfully: {}", email);

        Ok(RegisterResponse {
            user_id,
            email: email.clone(),
            email_verified: false,
        })
    }

    async fn create_user_password(&self, user: &DomainUser, password_hash: &PasswordHash) -> Result<(), sqlx::Error> {
        let query = r#"
            INSERT INTO user_passwords (user_id, password_hash, updated_at)
            VALUES ($1, $2, $3)
        "#;

        let password_hash_str = password_hash.as_str().to_string();
        sqlx::query(query)
            .bind(user.id)
            .bind(&password_hash_str)
            .bind(Utc::now())
            .execute(&self.user_repository.get_pool())
            .await?;

        Ok(())
    }
}