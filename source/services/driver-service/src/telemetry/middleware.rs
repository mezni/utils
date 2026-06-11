use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use actix_web::Result;
use std::time::Instant;
use tracing::info;

pub async fn logging_middleware(
    req: ServiceRequest,
    next: Next<impl actix_web::body::MessageBody>,
) -> Result<ServiceResponse<impl actix_web::body::MessageBody>> {
    let start = Instant::now();
    let method = req.method().to_string();
    let path = req.path().to_string();

    let res = next.call(req).await;

    let duration = start.elapsed();
    let status = match &res {
        Ok(r) => r.status().as_u16(),
        Err(e) => e.as_response_error().status_code().as_u16(),
    };

    info!(
        method = %method,
        path = %path,
        status = status,
        duration_ms = duration.as_millis() as u64,
        "request completed"
    );

    res
}
