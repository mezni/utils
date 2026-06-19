use actix_web::{dev::RequestHead, Error, FromRequest, HttpRequest};
use redis::aio::ConnectionManager;
use uuid::Uuid;
use crate::redis::{get_idempotency_key, store_idempotency_key};
use crate::error::AuthError;

#[derive(Clone)]
pub struct IdempotencyKey {
    pub key: String,
    pub is_replay: bool,
}

impl FromRequest for IdempotencyKey {
    type Error = Error;
    type Future = std::future::Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, head: &mut RequestHead) -> Self::Future {
        // Only validate idempotency for POST/PUT/DELETE methods
        let method = req.method();
        if !matches!(method.as_str(), "POST" | "PUT" | "DELETE") {
            return std::future::ready(Ok(IdempotencyKey {
                key: Uuid::new_v4().to_string(),
                is_replay: false,
            }));
        }

        // Extract Idempotency-Key header
        let idempotency_key = req
            .headers()
            .get("Idempotency-Key")
            .and_then(|header| header.to_str().ok())
            .ok_or_else(|| AuthError::Forbidden(
                "Idempotency-Key header is required for POST/PUT/DELETE operations".to_string()
            ))?;

        // Validate UUID v4 format
        let uuid_result: Result<Uuid, _> = idempotency_key.parse();
        if uuid_result.is_err() {
            return std::future::ready(Err(AuthError::ValidationError(
                "Invalid UUID format for Idempotency-Key".to_string()
            ).into()));
        }

        let is_replay = false; // Will be updated if key exists

        std::future::ready(Ok(IdempotencyKey {
            key: idempotency_key.to_string(),
            is_replay,
        }))
    }
}

#[cfg(test)]
mod tests {
    use actix_web::test::{call_service, TestRequest};
    use super::*;

    #[actix_web::test]
    async fn test_idempotency_without_key() {
        let req = TestRequest::post()
            .insert_header((header::CONTENT_TYPE, "application/json"))
            .to_http_request();

        let result = call_service(&req, IdempotencyKey::from_request(&req, &req.head()).await);
        assert!(result.is_err());
    }

    #[actix_web::test]
    async fn test_idempotency_with_valid_uuid() {
        let req = TestRequest::post()
            .insert_header((header::CONTENT_TYPE, "application/json"))
            .insert_header(("Idempotency-Key", "550e8400-e29b-41d4-a716-446655440000"))
            .to_http_request();

        let result = call_service(&req, IdempotencyKey::from_request(&req, &req.head()).await);
        assert!(result.is_ok());
    }
}
