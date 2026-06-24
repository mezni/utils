use thiserror::Error;

#[derive(Error, Debug)]
pub enum NearbyError {
    #[error("validation error: {0}")]
    Validation(String),

    #[error("internal error")]
    Internal,
}

impl From<sqlx::Error> for NearbyError {
    fn from(_: sqlx::Error) -> Self {
        NearbyError::Internal
    }
}
