use thiserror::Error;

#[derive(Debug, Error)]
pub enum DomainError {
    #[error("Account not found")]
    NotFound,
    #[error("Account already exists")]
    AlreadyExists,
    #[error("Invalid credentials")]
    InvalidCredentials,
    #[error("Invalid email format")]
    InvalidEmail,
    #[error("Password must be at least 8 characters")]
    WeakPassword,
}
