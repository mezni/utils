use actix_web::http::StatusCode;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Internal server error: {0}")]
    InternalError(String),

    #[error("Service unavailable: {0}")]
    ServiceUnavailable(String),
}

impl AppError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::ValidationError(_) => StatusCode::UNPROCESSABLE_ENTITY,
            AppError::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    pub fn error_code(&self) -> &'static str {
        match self {
            AppError::NotFound(_) => "not_found",
            AppError::ValidationError(_) => "validation_error",
            AppError::InternalError(_) => "internal_error",
            AppError::ServiceUnavailable(_) => "service_unavailable",
        }
    }
}

impl From<borne_data::DataLayerError> for AppError {
    fn from(e: borne_data::DataLayerError) -> Self {
        match e {
            borne_data::DataLayerError::NotFound(msg) => AppError::NotFound(msg),
            borne_data::DataLayerError::Connection(msg) => AppError::ServiceUnavailable(msg),
            borne_data::DataLayerError::PoolExhausted => {
                AppError::ServiceUnavailable("Connection pool exhausted".into())
            }
            borne_data::DataLayerError::Query(msg) | borne_data::DataLayerError::Migration(msg) => {
                AppError::InternalError(msg)
            }
        }
    }
}

impl actix_web::error::ResponseError for AppError {
    fn status_code(&self) -> StatusCode {
        self.status_code()
    }

    fn error_response(&self) -> actix_web::HttpResponse {
        let dto = crate::dto::error_response::ErrorResponse::from(self);
        actix_web::HttpResponse::build(self.status_code()).json(dto)
    }
}
