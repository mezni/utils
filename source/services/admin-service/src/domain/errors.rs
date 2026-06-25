use thiserror::Error;

#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum ServiceError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("validation error: {0}")]
    Validation(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("internal error")]
    Internal,
}

impl From<sqlx::Error> for ServiceError {
    fn from(e: sqlx::Error) -> Self {
        tracing::error!(error = %e, "database error");
        match e {
            sqlx::Error::RowNotFound => ServiceError::NotFound("resource".into()),
            _ => ServiceError::Internal,
        }
    }
}
