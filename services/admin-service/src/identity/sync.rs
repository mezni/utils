use actix_web::body::BoxBody;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use actix_web::HttpMessage;
use domain_types::jwt::JwtClaims;
use std::sync::Arc;

use crate::middleware::jwt::JwtMiddleware;

#[derive(Clone)]
pub struct SyncMiddleware {
    auth_sync_url: String,
    jwt_middleware: Arc<JwtMiddleware>,
}

impl SyncMiddleware {
    pub fn new(auth_sync_url: impl Into<String>, jwt_middleware: Arc<JwtMiddleware>) -> Self {
        Self {
            auth_sync_url: auth_sync_url.into(),
            jwt_middleware,
        }
    }
}

pub async fn identity_sync_middleware(
    req: ServiceRequest,
    next: Next<BoxBody>,
) -> Result<ServiceResponse<BoxBody>, actix_web::Error> {
    if req.path() == "/health" {
        return next.call(req).await;
    }

    let has_claims = req.extensions().get::<JwtClaims>().is_some();
    if has_claims {
        return next.call(req).await;
    }

    next.call(req).await
}
