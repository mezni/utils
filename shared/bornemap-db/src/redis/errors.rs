use thiserror::Error;

#[derive(Debug, Error)]
pub enum RedisError {
    #[error("Redis connection failed: {0}")]
    Connection(String),

    #[error("Redis command failed: {0}")]
    Command(String),

    #[error("Redis timeout: {0}")]
    Timeout(String),

    #[error("Redis authentication failed: {0}")]
    Authentication(String),

    #[error("Redis configuration error: {0}")]
    Configuration(String),

    #[error("Redis network error: {0}")]
    Network(String),

    #[error("Redis server error: {0}")]
    Server(String),

    #[error("Redis key not found: {0}")]
    KeyNotFound(String),

    #[error("Redis value expired: {0}")]
    ValueExpired(String),

    #[error("Redis rate limit exceeded: {0}")]
    RateLimitExceeded(String),
}

impl RedisError {
    pub fn connection<S: Into<String>>(msg: S) -> Self {
        Self::Connection(msg.into())
    }

    pub fn command<S: Into<String>>(msg: S) -> Self {
        Self::Command(msg.into())
    }

    pub fn timeout<S: Into<String>>(msg: S) -> Self {
        Self::Timeout(msg.into())
    }

    pub fn authentication<S: Into<String>>(msg: S) -> Self {
        Self::Authentication(msg.into())
    }

    pub fn configuration<S: Into<String>>(msg: S) -> Self {
        Self::Configuration(msg.into())
    }

    pub fn network<S: Into<String>>(msg: S) -> Self {
        Self::Network(msg.into())
    }

    pub fn server<S: Into<String>>(msg: S) -> Self {
        Self::Server(msg.into())
    }

    pub fn key_not_found<S: Into<String>>(msg: S) -> Self {
        Self::KeyNotFound(msg.into())
    }

    pub fn value_expired<S: Into<String>>(msg: S) -> Self {
        Self::ValueExpired(msg.into())
    }

    pub fn rate_limit_exceeded<S: Into<String>>(msg: S) -> Self {
        Self::RateLimitExceeded(msg.into())
    }
}

pub type RedisResult<T> = Result<T, RedisError>;

// Convert Redis errors to AppError
impl From<RedisError> for bornemap_core::AppError {
    fn from(e: RedisError) -> Self {
        match e {
            RedisError::Connection(_) => bornemap_core::AppError::ConfigurationError(format!("Redis connection failed: {}", e)),
            RedisError::Command(_) => bornemap_core::AppError::InternalError,
            RedisError::Timeout(_) => bornemap_core::AppError::InternalError,
            RedisError::Authentication(_) => bornemap_core::AppError::ConfigurationError(format!("Redis authentication failed: {}", e)),
            RedisError::Configuration(_) => bornemap_core::AppError::ConfigurationError(e.to_string()),
            RedisError::Network(_) => bornemap_core::AppError::InternalError,
            RedisError::Server(_) => bornemap_core::AppError::InternalError,
            RedisError::KeyNotFound(_) => bornemap_core::AppError::NotFound,
            RedisError::ValueExpired(_) => bornemap_core::AppError::InvalidSession,
            RedisError::RateLimitExceeded(_) => bornemap_core::AppError::ValidationError(format!("Rate limit exceeded: {}", e)),
        }
    }
}