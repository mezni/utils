use serde::Serialize;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Validation Error: {0}")]
    Validation(String),

    #[error("Not Found: {0}")]
    NotFound(String),

    #[error("Internal Error: {0}")]
    Internal(String),

    #[error("Database Error: {0}")]
    Database(String),

    #[error("Serialization Error: {0}")]
    Serialization(String),

    #[error("Configuration Error: {0}")]
    Configuration(String),

    #[error("IO Error: {0}")]
    Io(String),
}

impl Serialize for AppError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl From<sqlx::Error> for AppError {
    fn from(e: sqlx::Error) -> Self {
        AppError::Database(e.to_string())
    }
}

impl From<serde_json::Error> for AppError {
    fn from(e: serde_json::Error) -> Self {
        AppError::Serialization(e.to_string())
    }
}

impl From<std::io::Error> for AppError {
    fn from(e: std::io::Error) -> Self {
        AppError::Io(e.to_string())
    }
}

pub type AppResult<T> = Result<T, AppError>;

pub fn validation_error(msg: &str) -> AppError {
    AppError::Validation(msg.to_string())
}

pub fn not_found_error(msg: &str) -> AppError {
    AppError::NotFound(msg.to_string())
}

pub fn internal_error(msg: &str) -> AppError {
    AppError::Internal(msg.to_string())
}
