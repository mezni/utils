use thiserror::Error;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("Invalid token: {0}")]
    InvalidToken(String),

    #[error("Expired token")]
    ExpiredToken,

    #[error("Forbidden: {0}")]
    Forbidden(String),

    #[error("Missing credentials")]
    MissingCredentials,

    #[error("Invalid API key")]
    InvalidApiKey,
}
