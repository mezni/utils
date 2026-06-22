use actix_web::body::BoxBody;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use actix_web::HttpMessage;
use domain_types::audit::AuditEvent;
use domain_types::jwt::JwtClaims;
use std::sync::Arc;

use super::emitter::{create_idempotency_key, AuditEmitter};

pub async fn audit_middleware(
    req: ServiceRequest,
    next: Next<BoxBody>,
    emitter: Arc<AuditEmitter>,
) -> Result<ServiceResponse<BoxBody>, actix_web::Error> {
    if req.path() == "/health" {
        return next.call(req).await;
    }

    let claims = req.extensions().get::<JwtClaims>().cloned();
    let path = req.path().to_string();
    let method = req.method().as_str().to_string();

    let result = next.call(req).await;

    if let Some(claims) = claims {
        let event_type = if result.is_ok() {
            "auth.access_granted"
        } else {
            "auth.access_denied"
        };

        let event = AuditEvent::new(
            event_type,
            "auth-service",
            serde_json::json!({
                "user_uuid": claims.sub,
                "role": claims.role.as_str(),
                "path": path,
                "method": method,
            }),
            create_idempotency_key(event_type, &claims.sub),
        );

        tokio::spawn({
            let emitter = emitter.clone();
            async move {
                if let Err(e) = emitter.emit(event).await {
                    tracing::warn!("Audit emit failed: {}", e);
                }
            }
        });
    }

    result
}
