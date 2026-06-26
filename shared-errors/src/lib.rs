use shared_contracts::{AuthError, ValidationError};
use thiserror::Error;
use tracing::{error, warn};

#[derive(Error, Debug)]
pub enum AuthServiceError {
    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("User not found")]
    UserNotFound,

    #[error("Invalid credentials")]
    InvalidCredentials,

    #[error("User already exists: {0}")]
    UserAlreadyExists(String),

    #[error("Password validation failed: {0}")]
    PasswordValidationError(String),

    #[error("Refresh token invalid: {0}")]
    InvalidRefreshToken(String),

    #[error("Refresh token expired: {0}")]
    RefreshTokenExpired(String),

    #[error("Refresh token revoked: {0}")]
    RefreshTokenRevoked(String),

    #[error("Refresh token reuse detected")]
    TokenReuseDetected,

    #[error("Token validation failed: {0}")]
    TokenValidationError(String),

    #[error("Validation error: {0}")]
    ValidationError(#[from] ValidationError),

    #[error("Database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    #[error("Redis error: {0}")]
    CacheError(#[from] redis::RedisError),

    #[error("JWT error: {0}")]
    JwtError(#[from] jsonwebtoken::errors::Error),

    #[error("Internal server error: {0}")]
    InternalError(String),

    #[error("Rate limit exceeded")]
    RateLimitExceeded,
}

impl AuthError {
    pub fn from_service_error(error: &AuthServiceError) -> AuthError {
        match error {
            AuthServiceError::AuthenticationFailed(msg) => {
                AuthError::new("AUTH_FAILED", msg.clone())
            }
            AuthServiceError::UserNotFound => {
                AuthError::new("USER_NOT_FOUND", "User not found".to_string())
            }
            AuthServiceError::InvalidCredentials => AuthError::new(
                "INVALID_CREDENTIALS",
                "Invalid email or password".to_string(),
            ),
            AuthServiceError::UserAlreadyExists(email) => AuthError::new(
                "USER_EXISTS",
                format!("User with email {} already exists", email),
            )
            .with_details(vec!["email".to_string()]),
            AuthServiceError::PasswordValidationError(msg) => {
                AuthError::new("PASSWORD_ERROR", msg.clone())
                    .with_details(vec!["password".to_string()])
            }
            AuthServiceError::InvalidRefreshToken(token) => {
                AuthError::new("INVALID_TOKEN", format!("Invalid refresh token: {}", token))
                    .with_details(vec!["refresh_token".to_string()])
            }
            AuthServiceError::RefreshTokenExpired(token) => {
                AuthError::new("TOKEN_EXPIRED", format!("Refresh token expired: {}", token))
                    .with_details(vec!["refresh_token".to_string()])
            }
            AuthServiceError::RefreshTokenRevoked(token) => {
                AuthError::new("TOKEN_REVOKED", format!("Refresh token revoked: {}", token))
                    .with_details(vec!["refresh_token".to_string()])
            }
            AuthServiceError::TokenReuseDetected => AuthError::new(
                "TOKEN_REUSE",
                "Refresh token reuse detected. Token revoked immediately.".to_string(),
            ),
            AuthServiceError::TokenValidationError(msg) => {
                AuthError::new("TOKEN_ERROR", msg.clone())
            }
            AuthServiceError::RateLimitExceeded => AuthError::new(
                "RATE_LIMIT",
                "Too many requests. Please try again later.".to_string(),
            ),
            AuthServiceError::ValidationError(_) => {
                AuthError::new("VALIDATION_ERROR", "Invalid input data".to_string())
            }
            AuthServiceError::DatabaseError(_) => {
                AuthError::new("DATABASE_ERROR", "Database operation failed".to_string())
            }
            AuthServiceError::CacheError(_) => {
                AuthError::new("CACHE_ERROR", "Cache operation failed".to_string())
            }
            AuthServiceError::JwtError(_) => {
                AuthError::new("JWT_ERROR", "Token validation failed".to_string())
            }
            AuthServiceError::InternalError(msg) => {
                error!("Internal error: {}", msg);
                AuthError::new("INTERNAL_ERROR", msg.clone())
            }
        }
    }
}

pub fn handle_error(error: AuthServiceError) -> (AuthError, Option<Vec<String>>) {
    let auth_error = AuthError::from_service_error(&error);

    match &error {
        AuthServiceError::UserNotFound => warn!("User not found"),
        AuthServiceError::InvalidCredentials => warn!("Invalid credentials attempt"),
        AuthServiceError::UserAlreadyExists(_) => {
            warn!("User creation attempt with existing email")
        }
        AuthServiceError::RefreshTokenExpired(_) => warn!("Expired refresh token attempt"),
        AuthServiceError::RefreshTokenRevoked(_) => warn!("Revoked refresh token attempt"),
        AuthServiceError::TokenReuseDetected => {
            error!("CRITICAL: Token reuse detected - security breach attempt");
        }
        AuthServiceError::InternalError(msg) => error!("Internal error: {}", msg),
        _ => {}
    }

    (auth_error, auth_error.details.clone())
}
