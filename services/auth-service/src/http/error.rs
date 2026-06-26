use actix_web::HttpResponse;
use bornemap_core::AppError;
use tracing::error;

use crate::response::{
    ApiError, ApiResponse, CONFIGURATION_ERROR, DATABASE_ERROR, FORBIDDEN_ERROR, INTERNAL_ERROR,
    INVALID_CREDENTIALS, NOT_FOUND_ERROR, UNAUTHORIZED_ERROR, USER_ALREADY_EXISTS, USER_NOT_FOUND,
    VALIDATION_ERROR,
};

pub fn map_app_error(err: AppError) -> HttpResponse {
    let (status, api_error) = match &err {
        AppError::Unauthorized => (
            401,
            ApiError {
                code: UNAUTHORIZED_ERROR.to_string(),
                message: "Unauthorized".to_string(),
                field: None,
            },
        ),
        AppError::InvalidCredentials => (
            401,
            ApiError {
                code: INVALID_CREDENTIALS.to_string(),
                message: "Invalid credentials".to_string(),
                field: None,
            },
        ),
        AppError::UserAlreadyExists => (
            409,
            ApiError {
                code: USER_ALREADY_EXISTS.to_string(),
                message: "User already exists".to_string(),
                field: None,
            },
        ),
        AppError::UserNotFound => (
            404,
            ApiError {
                code: USER_NOT_FOUND.to_string(),
                message: "User not found".to_string(),
                field: None,
            },
        ),
        AppError::TokenError(msg) => (
            401,
            ApiError {
                code: UNAUTHORIZED_ERROR.to_string(),
                message: "Token error".to_string(),
                field: Some(msg.clone()),
            },
        ),
        AppError::InvalidSession => (
            401,
            ApiError {
                code: UNAUTHORIZED_ERROR.to_string(),
                message: "Invalid session".to_string(),
                field: None,
            },
        ),
        AppError::ExpiredSession => (
            401,
            ApiError {
                code: UNAUTHORIZED_ERROR.to_string(),
                message: "Session expired".to_string(),
                field: None,
            },
        ),
        AppError::ConfigurationError(msg) => (
            500,
            ApiError {
                code: CONFIGURATION_ERROR.to_string(),
                message: "Configuration error".to_string(),
                field: Some(msg.clone()),
            },
        ),
        AppError::DatabaseError(msg) => (
            500,
            ApiError {
                code: DATABASE_ERROR.to_string(),
                message: "Database error".to_string(),
                field: Some(msg.clone()),
            },
        ),
        AppError::InternalError => (
            500,
            ApiError {
                code: INTERNAL_ERROR.to_string(),
                message: "Internal server error".to_string(),
                field: None,
            },
        ),
        AppError::ValidationError(msg) => (
            400,
            ApiError {
                code: VALIDATION_ERROR.to_string(),
                message: "Validation error".to_string(),
                field: Some(msg.clone()),
            },
        ),
        AppError::Forbidden => (
            403,
            ApiError {
                code: FORBIDDEN_ERROR.to_string(),
                message: "Forbidden".to_string(),
                field: None,
            },
        ),
        AppError::NotFound => (
            404,
            ApiError {
                code: NOT_FOUND_ERROR.to_string(),
                message: "Resource not found".to_string(),
                field: None,
            },
        ),
    };

    error!("Application error: {:?}", err);

    HttpResponse::build(
        actix_web::http::StatusCode::from_u16(status)
            .unwrap_or(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR),
    )
    .json(ApiResponse::<()>::error(
        api_error.code,
        api_error.message,
        "unknown".to_string(), // Will be set by middleware
        api_error.field,
    ))
}

pub fn map_validation_errors(errors: Vec<String>) -> HttpResponse {
    let _status = 400u16;
    let api_error = ApiError {
        code: VALIDATION_ERROR.to_string(),
        message: "Validation failed".to_string(),
        field: Some(errors.join(", ")),
    };

    HttpResponse::build(actix_web::http::StatusCode::BAD_REQUEST).json(ApiResponse::<()>::error(
        api_error.code,
        api_error.message,
        "unknown".to_string(), // Will be set by middleware
        api_error.field,
    ))
}
