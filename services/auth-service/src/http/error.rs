use actix_web::{HttpResponse, HttpResponseBuilder};
use bornemap_core::{AppError, AuthError};

use super::dto::{ErrorDetail, ErrorResponse};

pub fn map_auth_error(err: AuthError) -> HttpResponse {
    let (status, code, message) = match &err {
        AuthError::InvalidCredentials => (409, "INVALID_CREDENTIALS", err.to_string()),
        AuthError::EmailAlreadyExists => (409, "EMAIL_ALREADY_EXISTS", err.to_string()),
        AuthError::UserNotFound => (404, "USER_NOT_FOUND", err.to_string()),
        AuthError::ValidationError(msg) => (400, "VALIDATION_ERROR", msg.clone()),
        AuthError::Unauthorized => (401, "UNAUTHORIZED", err.to_string()),
        AuthError::InternalError => (500, "INTERNAL_ERROR", err.to_string()),
    };

    error_response(status, code, &message)
}

pub fn map_app_error(err: AppError) -> HttpResponse {
    let (status, code, message) = match &err {
        AppError::Unauthorized => (401, "UNAUTHORIZED", err.to_string()),
        AppError::InvalidCredentials => (409, "INVALID_CREDENTIALS", err.to_string()),
        AppError::UserAlreadyExists => (409, "USER_ALREADY_EXISTS", err.to_string()),
        AppError::TokenError(_) => (401, "TOKEN_ERROR", err.to_string()),
        AppError::InvalidSession => (401, "INVALID_SESSION", err.to_string()),
        AppError::ExpiredSession => (401, "EXPIRED_SESSION", err.to_string()),
        AppError::ConfigurationError(msg) => (500, "CONFIGURATION_ERROR", msg.clone()),
        AppError::DatabaseError(msg) => (500, "DATABASE_ERROR", msg.clone()),
        AppError::InternalError => (500, "INTERNAL_ERROR", err.to_string()),
        AppError::ValidationError(msg) => (400, "VALIDATION_ERROR", msg.clone()),
    };

    error_response(status, code, &message)
}

fn error_response(status: u16, code: &str, message: &str) -> HttpResponse {
    HttpResponseBuilder::new(
        actix_web::http::StatusCode::from_u16(status)
            .unwrap_or(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR),
    )
    .json(ErrorResponse {
        error: ErrorDetail {
            code: code.into(),
            message: message.into(),
        },
    })
}
