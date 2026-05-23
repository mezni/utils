use thiserror::Error;

#[derive(Error, Debug)]
pub enum CoreServiceError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    #[error("Forbidden: {0}")]
    Forbidden(String),

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Internal server error: {0}")]
    Internal(String),

    #[error("Bad request: {0}")]
    BadRequest(String),

    #[error("Service unavailable: {0}")]
    ServiceUnavailable(String),

    #[error("Configuration error: {0}")]
    Configuration(String),
}

impl CoreServiceError {
    pub fn status_code(&self) -> actix_web::http::StatusCode {
        match self {
            CoreServiceError::Database(_) => actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
            CoreServiceError::Validation(_) => actix_web::http::StatusCode::BAD_REQUEST,
            CoreServiceError::NotFound(_) => actix_web::http::StatusCode::NOT_FOUND,
            CoreServiceError::Unauthorized(_) => actix_web::http::StatusCode::UNAUTHORIZED,
            CoreServiceError::Forbidden(_) => actix_web::http::StatusCode::FORBIDDEN,
            CoreServiceError::Conflict(_) => actix_web::http::StatusCode::CONFLICT,
            CoreServiceError::Internal(_) => actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
            CoreServiceError::BadRequest(_) => actix_web::http::StatusCode::BAD_REQUEST,
            CoreServiceError::ServiceUnavailable(_) => actix_web::http::StatusCode::SERVICE_UNAVAILABLE,
            CoreServiceError::Configuration(_) => actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn error_type(&self) -> &'static str {
        match self {
            CoreServiceError::Database(_) => "database-error",
            CoreServiceError::Validation(_) => "validation-error",
            CoreServiceError::NotFound(_) => "not-found",
            CoreServiceError::Unauthorized(_) => "unauthorized",
            CoreServiceError::Forbidden(_) => "forbidden",
            CoreServiceError::Conflict(_) => "conflict",
            CoreServiceError::Internal(_) => "internal-error",
            CoreServiceError::BadRequest(_) => "bad-request",
            CoreServiceError::ServiceUnavailable(_) => "service-unavailable",
            CoreServiceError::Configuration(_) => "configuration-error",
        }
    }
}

impl actix_web::ResponseError for CoreServiceError {
    fn error_response(&self) -> actix_web::HttpResponse {
        let status_code = self.status_code();
        let error_response = ErrorResponse {
            type_: format!("https://api.bornemap.tn/errors/{}", self.error_type()),
            title: self.to_string(),
            status: status_code.as_u16(),
            detail: self.to_string(),
            instance: "".to_string(), // Will be set by the middleware
            errors: match self {
                CoreServiceError::Validation(msg) => {
                    vec![ErrorDetail {
                        field: "validation".to_string(),
                        message: msg.clone(),
                    }]
                }
                _ => vec![],
            },
        };

        actix_web::HttpResponse::build(status_code)
            .content_type("application/problem+json")
            .json(error_response)
    }
}

#[derive(Debug, serde::Serialize)]
pub struct ErrorResponse {
    #[serde(rename = "type")]
    pub type_: String,
    pub title: String,
    pub status: u16,
    pub detail: String,
    pub instance: String,
    pub errors: Vec<ErrorDetail>,
}

#[derive(Debug, serde::Serialize)]
pub struct ErrorDetail {
    pub field: String,
    pub message: String,
}

/// Helper function to create validation errors
pub fn validation_error(field: &str, message: &str) -> CoreServiceError {
    CoreServiceError::Validation(format!("{}: {}", field, message))
}

/// Helper function to create not found errors
pub fn not_found(resource: &str) -> CoreServiceError {
    CoreServiceError::NotFound(format!("{} not found", resource))
}

/// Helper function to create unauthorized errors
pub fn unauthorized(message: &str) -> CoreServiceError {
    CoreServiceError::Unauthorized(message.to_string())
}

/// Helper function to create forbidden errors
pub fn forbidden(message: &str) -> CoreServiceError {
    CoreServiceError::Forbidden(message.to_string())
}

/// Helper function to create conflict errors
pub fn conflict(message: &str) -> CoreServiceError {
    CoreServiceError::Conflict(message.to_string())
}

/// Helper function to create bad request errors
pub fn bad_request(message: &str) -> CoreServiceError {
    CoreServiceError::BadRequest(message.to_string())
}

/// Helper function to create internal server errors
pub fn internal_error(message: &str) -> CoreServiceError {
    CoreServiceError::Internal(message.to_string())
}

/// Helper function to create service unavailable errors
pub fn service_unavailable(message: &str) -> CoreServiceError {
    CoreServiceError::ServiceUnavailable(message.to_string())
}

/// Helper function to create configuration errors
pub fn configuration_error(message: &str) -> CoreServiceError {
    CoreServiceError::Configuration(message.to_string())
}

/// Result type for core service operations
pub type CoreResult<T> = Result<T, CoreServiceError>;