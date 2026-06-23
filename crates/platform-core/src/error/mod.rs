//! Error types for platform-core
//! Defines AppError enum with variants for different error categories

use serde::Serialize;
use thiserror::Error;

/// Application-wide error type
#[derive(Error, Serialize, Debug, Clone, PartialEq)]
pub enum AppError {
    #[error("Validation Error: {0}")]
    Validation(String),

    #[error("Not Found: {0}")]
    NotFound(String),

    #[error("Internal Error: {0}")]
    Internal(String),

    #[error("Database Error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Serialization Error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Configuration Error: {0}")]
    Configuration(String),

    #[error("IO Error: {0}")]
    Io(#[from] std::io::Error),
}

/// Convenience type alias for Result type
pub type AppResult<T> = Result<T, AppError>;

/// Helper function to create validation errors
pub fn validation_error(msg: &str) -> AppError {
    AppError::Validation(msg.to_string())
}

/// Helper function to create not found errors
pub fn not_found_error(msg: &str) -> AppError {
    AppError::NotFound(msg.to_string())
}

/// Helper function to create internal errors
pub fn internal_error(msg: &str) -> AppError {
    AppError::Internal(msg.to_string())
}
