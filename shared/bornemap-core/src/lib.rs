use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub type UserId = Uuid;
pub type SessionId = Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum UserRole {
    RegisteredDriver,
    Partner,
    Admin,
}

impl UserRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            UserRole::RegisteredDriver => "REGISTERED_DRIVER",
            UserRole::Partner => "PARTNER",
            UserRole::Admin => "ADMIN",
        }
    }

    pub fn try_from_str(s: &str) -> Option<Self> {
        match s {
            "REGISTERED_DRIVER" => Some(UserRole::RegisteredDriver),
            "PARTNER" => Some(UserRole::Partner),
            "ADMIN" => Some(UserRole::Admin),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum UserStatus {
    Active,
    Suspended,
    Deleted,
}

impl UserStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            UserStatus::Active => "ACTIVE",
            UserStatus::Suspended => "SUSPENDED",
            UserStatus::Deleted => "DELETED",
        }
    }

    pub fn try_from_str(s: &str) -> Option<Self> {
        match s {
            "ACTIVE" => Some(UserStatus::Active),
            "SUSPENDED" => Some(UserStatus::Suspended),
            "DELETED" => Some(UserStatus::Deleted),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: UserId,
    pub email: String,
    pub password_hash: String,
    pub role: UserRole,
    pub status: UserStatus,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("Invalid credentials")]
    InvalidCredentials,

    #[error("Email already exists")]
    EmailAlreadyExists,

    #[error("User not found")]
    UserNotFound,

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Unauthorized")]
    Unauthorized,

    #[error("Internal error")]
    InternalError,
}

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Unauthorized")]
    Unauthorized,

    #[error("Invalid credentials")]
    InvalidCredentials,

    #[error("User already exists")]
    UserAlreadyExists,

    #[error("Token error: {0}")]
    TokenError(String),

    #[error("Invalid session")]
    InvalidSession,

    #[error("Expired session")]
    ExpiredSession,

    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Internal error")]
    InternalError,

    #[error("Validation error: {0}")]
    ValidationError(String),
}

impl From<AuthError> for AppError {
    fn from(e: AuthError) -> Self {
        match e {
            AuthError::InvalidCredentials => AppError::InvalidCredentials,
            AuthError::EmailAlreadyExists => AppError::UserAlreadyExists,
            AuthError::UserNotFound => AppError::InvalidCredentials,
            AuthError::ValidationError(msg) => AppError::ValidationError(msg),
            AuthError::Unauthorized => AppError::Unauthorized,
            AuthError::InternalError => AppError::InternalError,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: SessionId,
    pub user_id: UserId,
    pub token_hash: String,
    pub family_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub last_used_at: DateTime<Utc>,
    pub revoked: bool,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[async_trait]
pub trait UserRepository: Send + Sync {
    async fn create(&self, user: &User) -> Result<(), AuthError>;
    async fn find_by_email(&self, email: &str) -> Result<Option<User>, AuthError>;
    async fn find_by_id(&self, id: UserId) -> Result<Option<User>, AuthError>;
    async fn email_exists(&self, email: &str) -> Result<bool, AuthError>;
}

#[async_trait]
pub trait SessionRepository: Send + Sync {
    async fn create(&self, session: &Session) -> Result<(), AppError>;
    async fn find_by_token_hash(&self, token_hash: &str) -> Result<Option<Session>, AppError>;
    async fn revoke_session(&self, id: SessionId) -> Result<(), AppError>;
    async fn revoke_family(&self, family_id: Uuid) -> Result<(), AppError>;
    async fn delete_expired(&self) -> Result<u64, AppError>;
}
