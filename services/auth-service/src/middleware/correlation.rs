use actix_web::body::BoxBody;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::http::header::{HeaderName, HeaderValue};
use actix_web::middleware::Next;
use uuid::Uuid;

pub async fn correlation_middleware(
    mut req: ServiceRequest,
    next: Next<BoxBody>,
) -> Result<ServiceResponse<BoxBody>, actix_web::Error> {
    let correlation_id = req
        .headers()
        .get("X-Correlation-ID")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("corr-{}", Uuid::new_v4()));

    let header_name = HeaderName::from_static("x-correlation-id");
    let header_value = HeaderValue::from_str(&correlation_id).unwrap();

    req.headers_mut().insert(header_name.clone(), header_value.clone());

    let mut res = next.call(req).await?;
    res.headers_mut()
        .insert(header_name, header_value);

    Ok(res)
}
