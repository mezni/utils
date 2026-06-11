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

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::http::StatusCode;
    use actix_web::ResponseError;

    #[test]
    fn not_found_status_and_code() {
        let err = AppError::NotFound("station_123".into());
        assert_eq!(err.status_code(), StatusCode::NOT_FOUND);
        assert_eq!(err.error_code(), "not_found");
        assert_eq!(err.to_string(), "Resource not found: station_123");
    }

    #[test]
    fn validation_error_status_and_code() {
        let err = AppError::ValidationError("lat out of range".into());
        assert_eq!(err.status_code(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(err.error_code(), "validation_error");
        assert_eq!(err.to_string(), "Validation error: lat out of range");
    }

    #[test]
    fn internal_error_status_and_code() {
        let err = AppError::InternalError("query failed".into());
        assert_eq!(err.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(err.error_code(), "internal_error");
        assert_eq!(err.to_string(), "Internal server error: query failed");
    }

    #[test]
    fn service_unavailable_status_and_code() {
        let err = AppError::ServiceUnavailable("db down".into());
        assert_eq!(err.status_code(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(err.error_code(), "service_unavailable");
        assert_eq!(err.to_string(), "Service unavailable: db down");
    }

    #[test]
    fn from_not_found() {
        let de = borne_data::DataLayerError::NotFound("x".into());
        let err = AppError::from(de);
        assert!(matches!(err, AppError::NotFound(_)));
        assert_eq!(err.error_code(), "not_found");
    }

    #[test]
    fn from_connection() {
        let de = borne_data::DataLayerError::Connection("timeout".into());
        let err = AppError::from(de);
        assert!(matches!(err, AppError::ServiceUnavailable(_)));
        assert_eq!(err.error_code(), "service_unavailable");
    }

    #[test]
    fn from_pool_exhausted() {
        let de = borne_data::DataLayerError::PoolExhausted;
        let err = AppError::from(de);
        assert!(matches!(err, AppError::ServiceUnavailable(_)));
        assert_eq!(
            err.to_string(),
            "Service unavailable: Connection pool exhausted"
        );
    }

    #[test]
    fn from_query() {
        let de = borne_data::DataLayerError::Query("syntax error".into());
        let err = AppError::from(de);
        assert!(matches!(err, AppError::InternalError(_)));
    }

    #[test]
    fn from_migration() {
        let de = borne_data::DataLayerError::Migration("failed".into());
        let err = AppError::from(de);
        assert!(matches!(err, AppError::InternalError(_)));
    }

    #[actix_web::test]
    async fn response_error_returns_json() {
        let err = AppError::NotFound("test".into());
        let resp: actix_web::HttpResponse = err.error_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let bytes = actix_web::body::to_bytes(resp.into_body()).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["error"]["code"], "not_found");
        assert!(v["data"].is_null());
        assert!(v["meta"].is_null());
    }
}
