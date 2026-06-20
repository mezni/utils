use std::fmt;

use actix_web::{HttpResponse, ResponseError};

#[derive(Debug)]
pub enum AppError {
    NotFound(String),
    ValidationError(String),
    DatabaseError(String),
    ServiceUnavailable(String),
    BadRequest(String),
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(msg) => write!(f, "Not found: {msg}"),
            Self::ValidationError(msg) => write!(f, "Validation error: {msg}"),
            Self::DatabaseError(msg) => write!(f, "Database error: {msg}"),
            Self::ServiceUnavailable(msg) => write!(f, "Service unavailable: {msg}"),
            Self::BadRequest(msg) => write!(f, "Bad request: {msg}"),
        }
    }
}

impl ResponseError for AppError {
    fn error_response(&self) -> HttpResponse {
        let (status, body) = match self {
            Self::NotFound(msg) => (actix_web::http::StatusCode::NOT_FOUND, msg.clone()),
            Self::ValidationError(msg) => {
                (actix_web::http::StatusCode::UNPROCESSABLE_ENTITY, msg.clone())
            }
            Self::DatabaseError(msg) => {
                (actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, msg.clone())
            }
            Self::ServiceUnavailable(msg) => {
                (actix_web::http::StatusCode::SERVICE_UNAVAILABLE, msg.clone())
            }
            Self::BadRequest(msg) => (actix_web::http::StatusCode::BAD_REQUEST, msg.clone()),
        };
        HttpResponse::build(status).json(serde_json::json!({ "error": body }))
    }
}

impl From<sqlx::Error> for AppError {
    fn from(err: sqlx::Error) -> Self {
        match &err {
            sqlx::Error::RowNotFound => {
                Self::NotFound("Resource not found".into())
            }
            sqlx::Error::Database(db_err) => {
                if let Some(code) = db_err.code() {
                    if code.as_ref() == "23503" {
                        return Self::ValidationError(
                            "Referenced entity does not exist".into(),
                        );
                    }
                    if code.as_ref() == "23505" {
                        return Self::ValidationError(
                            "Resource already exists (duplicate)".into(),
                        );
                    }
                }
                Self::DatabaseError(db_err.message().into())
            }
            _ => Self::ServiceUnavailable("Database connection failed".into()),
        }
    }
}
