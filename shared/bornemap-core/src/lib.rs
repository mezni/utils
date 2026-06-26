use thiserror::Error;
use uuid::Uuid;

pub type UserId = Uuid;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Unauthorized")]
    Unauthorized,

    #[error("Internal error")]
    InternalError,
}
