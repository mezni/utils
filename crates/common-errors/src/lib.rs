use actix_web::{HttpResponse, ResponseError};

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Bad request: {0}")]
    BadRequest(String),
    #[error("Unauthorized: {0}")]
    Unauthorized(String),
    #[error("Forbidden: {0}")]
    Forbidden(String),
    #[error("Conflict: {0}")]
    Conflict(String),
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("Database error: {0}")]
    Database(String),
}

impl ResponseError for AppError {
    fn error_response(&self) -> HttpResponse {
        let (status, message) = match self {
            AppError::NotFound(msg) => (actix_web::http::StatusCode::NOT_FOUND, msg.clone()),
            AppError::BadRequest(msg) => (actix_web::http::StatusCode::BAD_REQUEST, msg.clone()),
            AppError::Unauthorized(msg) => (actix_web::http::StatusCode::UNAUTHORIZED, msg.clone()),
            AppError::Forbidden(msg) => (actix_web::http::StatusCode::FORBIDDEN, msg.clone()),
            AppError::Conflict(msg) => (actix_web::http::StatusCode::CONFLICT, msg.clone()),
            AppError::Internal(msg) => {
                (actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, msg.clone())
            }
            AppError::Database(msg) => {
                (actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, msg.clone())
            }
        };

        HttpResponse::build(status).json(serde_json::json!({
            "error": message
        }))
    }
}

impl From<sqlx::Error> for AppError {
    fn from(err: sqlx::Error) -> Self {
        AppError::Database(err.to_string())
    }
}

pub type AppResult<T> = Result<T, AppError>;
