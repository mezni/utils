use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
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

    #[error("User not found")]
    UserNotFound,

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

    #[error("Forbidden")]
    Forbidden,

    #[error("Not found")]
    NotFound,

    // OAuth errors
    #[error("OAuth state is invalid")]
    OAuthStateInvalid,

    #[error("OAuth state has expired")]
    OAuthStateExpired,

    #[error("OAuth state has already been used")]
    OAuthStateReused,

    #[error("OAuth provider is unavailable")]
    OAuthProviderUnavailable(String),

    #[error("OAuth token exchange failed: {0}")]
    OAuthTokenExchangeFailed(String),

    #[error("OAuth profile fetch failed: {0}")]
    OAuthProfileFetchFailed(String),

    #[error("OAuth email is not verified")]
    OAuthEmailNotVerified,

    #[error("Unsupported OAuth provider: {0}")]
    UnsupportedOAuthProvider(String),

    #[error("OAuth account already exists")]
    OAuthAccountAlreadyExists,

    #[error("OAuth account linking failed: {0}")]
    OAuthAccountLinkFailed(String),

    #[error("OAuth state store error: {0}")]
    OAuthStateStoreError(String),
}

impl From<AuthError> for AppError {
    fn from(e: AuthError) -> Self {
        match e {
            AuthError::InvalidCredentials => AppError::InvalidCredentials,
            AuthError::EmailAlreadyExists => AppError::UserAlreadyExists,
            AuthError::UserNotFound => AppError::UserNotFound,
            AuthError::ValidationError(msg) => AppError::ValidationError(msg),
            AuthError::Unauthorized => AppError::Unauthorized,
            AuthError::InternalError => AppError::InternalError,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsersGrowthPoint {
    pub date: NaiveDate,
    pub count: i64,
}

pub struct UsersMetrics {
    pub total: i64,
    pub growth: Vec<UsersGrowthPoint>,
}

pub enum MetricsRange {
    Days7,
    Days30,
    Days90,
    Days365,
}

impl MetricsRange {
    pub fn num_days(&self) -> i64 {
        match self {
            MetricsRange::Days7 => 7,
            MetricsRange::Days30 => 30,
            MetricsRange::Days90 => 90,
            MetricsRange::Days365 => 365,
        }
    }

    pub fn from_str(s: &str) -> Result<Self, AppError> {
        match s {
            "7d" => Ok(MetricsRange::Days7),
            "30d" => Ok(MetricsRange::Days30),
            "90d" => Ok(MetricsRange::Days90),
            "365d" => Ok(MetricsRange::Days365),
            _ => Err(AppError::ValidationError(format!(
                "Invalid range '{}'. Supported values: 7d, 30d, 90d, 365d",
                s
            ))),
        }
    }
}

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
    async fn count_users(&self) -> Result<i64, AppError>;
    async fn users_growth_by_day(&self, range: &MetricsRange) -> Result<Vec<UsersGrowthPoint>, AppError>;
}

#[async_trait]
pub trait SessionRepository: Send + Sync {
    async fn create(&self, session: &Session) -> Result<(), AppError>;
    async fn find_by_token_hash(&self, token_hash: &str) -> Result<Option<Session>, AppError>;
    async fn revoke_session(&self, id: SessionId) -> Result<(), AppError>;
    async fn revoke_family(&self, family_id: Uuid) -> Result<(), AppError>;
    async fn delete_user_sessions(&self, user_id: UserId) -> Result<(), AppError>;
    async fn delete_expired(&self) -> Result<u64, AppError>;
}
