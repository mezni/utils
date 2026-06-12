use actix_web::{HttpResponse, ResponseError};
use serde::Serialize;
use std::fmt;

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: ErrorBody,
}

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Vec<FieldError>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FieldError {
    pub field: String,
    pub message: String,
}

#[derive(Debug)]
pub enum AppError {
    NotFound(String),
    Validation { details: Vec<FieldError> },
    BadRequest(String),
    Database(sqlx::Error),
    Internal(String),
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::NotFound(msg) => write!(f, "Not found: {}", msg),
            AppError::Validation { details } => {
                write!(f, "Validation error: {:?}", details)
            }
            AppError::BadRequest(msg) => write!(f, "Bad request: {}", msg),
            AppError::Database(e) => write!(f, "Database error: {}", e),
            AppError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl From<sqlx::Error> for AppError {
    fn from(e: sqlx::Error) -> Self {
        match &e {
            sqlx::Error::RowNotFound => AppError::NotFound("Resource not found".into()),
            _ => AppError::Database(e),
        }
    }
}

impl ResponseError for AppError {
    fn error_response(&self) -> HttpResponse {
        let (status, body) = match self {
            AppError::NotFound(msg) => (
                actix_web::http::StatusCode::NOT_FOUND,
                ErrorResponse {
                    error: ErrorBody {
                        code: "NOT_FOUND".into(),
                        message: msg.clone(),
                        details: None,
                    },
                },
            ),
            AppError::Validation { details } => (
                actix_web::http::StatusCode::BAD_REQUEST,
                ErrorResponse {
                    error: ErrorBody {
                        code: "VALIDATION_ERROR".into(),
                        message: "Request validation failed".into(),
                        details: Some(details.clone()),
                    },
                },
            ),
            AppError::BadRequest(msg) => (
                actix_web::http::StatusCode::BAD_REQUEST,
                ErrorResponse {
                    error: ErrorBody {
                        code: "BAD_REQUEST".into(),
                        message: msg.clone(),
                        details: None,
                    },
                },
            ),
            AppError::Database(e) => {
                tracing::error!("Database error: {}", e);
                (
                    actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                    ErrorResponse {
                        error: ErrorBody {
                            code: "INTERNAL_ERROR".into(),
                            message: "An unexpected database error occurred".into(),
                            details: None,
                        },
                    },
                )
            }
            AppError::Internal(msg) => {
                tracing::error!("Internal error: {}", msg);
                (
                    actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                    ErrorResponse {
                        error: ErrorBody {
                            code: "INTERNAL_ERROR".into(),
                            message: "An unexpected error occurred".into(),
                            details: None,
                        },
                    },
                )
            }
        };
        HttpResponse::build(status).json(body)
    }
}
