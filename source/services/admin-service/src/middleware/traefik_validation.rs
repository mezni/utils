use actix_web::{dev::RequestHead, Error, FromRequest, HttpRequest};
use crate::middleware::auth::extract_user_context;
use crate::error::AuthError;

pub struct TraefikHeaderValidation;

impl FromRequest for TraefikHeaderValidation {
    type Error = Error;
    type Future = std::future::Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _: &mut RequestHead) -> Self::Future {
        match extract_user_context(req) {
            Ok(_) => std::future::ready(Ok(TraefikHeaderValidation)),
            Err(e) => std::future::ready(Err(e.into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use actix_web::test::{call_service, TestRequest};
    use super::*;

    #[actix_web::test]
    async fn test_missing_x_user_id_header() {
        let req = TestRequest::get()
            .insert_header((header::CONTENT_TYPE, "application/json"))
            .to_http_request();

        let result = call_service(&req, TraefikHeaderValidation::from_request(&req, &req.head()).await);
        assert!(result.is_err());
    }

    #[actix_web::test]
    async fn test_missing_x_user_roles_header() {
        let req = TestRequest::get()
            .insert_header((header::CONTENT_TYPE, "application/json"))
            .insert_header((header::HeaderName::from_static("X-User-Id"), "USR-12345"))
            .to_http_request();

        let result = call_service(&req, TraefikHeaderValidation::from_request(&req, &req.head()).await);
        assert!(result.is_ok());
    }

    #[actix_web::test]
    async fn test_valid_headers() {
        let req = TestRequest::get()
            .insert_header((header::CONTENT_TYPE, "application/json"))
            .insert_header((header::HeaderName::from_static("X-User-Id"), "USR-12345"))
            .insert_header((header::HeaderName::from_static("X-User-Roles"), "role:admin,role:partner"))
            .to_http_request();

        let result = call_service(&req, TraefikHeaderValidation::from_request(&req, &req.head()).await);
        assert!(result.is_ok());
    }
}
