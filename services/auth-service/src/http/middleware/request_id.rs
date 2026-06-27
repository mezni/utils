use actix_web::dev::{ServiceRequest, ServiceResponse, Transform, Service};
use actix_web::http::header::{HeaderName, HeaderValue};
use actix_web::{dev::Payload, Error, FromRequest, HttpMessage, HttpRequest};
use futures::future::{ok, ready, Ready, LocalBoxFuture};
use std::task::{Context, Poll};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct RequestId(pub String);

impl RequestId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn from_header(header_value: &str) -> Option<Self> {
        if !header_value.is_empty() {
            Some(Self(header_value.to_string()))
        } else {
            None
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromRequest for RequestId {
    type Error = Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        if let Some(request_id) = req.extensions().get::<RequestId>() {
            return ready(Ok(request_id.clone()));
        }
        if let Some(header_value) = req.headers().get("X-Request-ID") {
            if let Ok(value) = header_value.to_str() {
                if let Some(request_id) = RequestId::from_header(value) {
                    return ready(Ok(request_id));
                }
            }
        }
        ready(Ok(RequestId::generate()))
    }
}

pub fn get_request_id(req: &ServiceRequest) -> String {
    if let Some(request_id) = req.extensions().get::<RequestId>() {
        return request_id.0.clone();
    }
    if let Some(header_value) = req.headers().get("X-Request-ID") {
        if let Ok(value) = header_value.to_str() {
            return value.to_string();
        }
    }
    Uuid::new_v4().to_string()
}

fn set_request_id_header(res: &mut ServiceResponse, request_id: &str) {
    let header_name = HeaderName::from_static("x-request-id");
    let header_value = HeaderValue::from_str(request_id)
        .unwrap_or_else(|_| HeaderValue::from_static("unknown"));
    res.headers_mut().insert(header_name, header_value);
}

pub struct RequestIdMiddleware;

impl<S> Transform<S, ServiceRequest> for RequestIdMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse, Error = Error> + 'static,
    S::Future: 'static,
{
    type Response = ServiceResponse;
    type Error = Error;
    type Transform = RequestIdMiddlewareService<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ok(RequestIdMiddlewareService { service })
    }
}

pub struct RequestIdMiddlewareService<S> {
    service: S,
}

impl<S> Service<ServiceRequest> for RequestIdMiddlewareService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse, Error = Error> + 'static,
    S::Future: 'static,
{
    type Response = ServiceResponse;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let request_id = get_request_id(&req);
        req.extensions_mut().insert(RequestId(request_id.clone()));
        let fut = self.service.call(req);
        Box::pin(async move {
            let mut res = fut.await?;
            set_request_id_header(&mut res, &request_id);
            Ok(res)
        })
    }
}
