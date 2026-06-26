use actix_web::{HttpResponse, HttpResponseBuilder};
use bornemap_core::AuthError;

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

    HttpResponseBuilder::new(
        actix_web::http::StatusCode::from_u16(status)
            .unwrap_or(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR),
    )
    .json(ErrorResponse {
        error: ErrorDetail {
            code: code.into(),
            message,
        },
    })
}
