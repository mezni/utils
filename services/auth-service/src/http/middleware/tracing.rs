use actix_web::dev::{ServiceRequest, ServiceResponse, Transform, Service};
use actix_web::Error;
use futures::future::{ok, Ready, LocalBoxFuture};
use std::task::{Context, Poll};
use tracing::{info_span, Instrument};

use super::request_id;

pub struct TracingMiddleware;

impl<S> Transform<S, ServiceRequest> for TracingMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse, Error = Error> + 'static,
    S::Future: 'static,
{
    type Response = ServiceResponse;
    type Error = Error;
    type Transform = TracingMiddlewareService<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ok(TracingMiddlewareService { service })
    }
}

pub struct TracingMiddlewareService<S> {
    service: S,
}

impl<S> Service<ServiceRequest> for TracingMiddlewareService<S>
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
        let request_id = request_id::get_request_id(&req);
        let method = req.method().to_string();
        let path = req.path().to_string();

        let span = info_span!(
            "http_request",
            request_id = %request_id,
            method = %method,
            path = %path,
            status = tracing::field::Empty,
            service = "auth-service",
        );

        let span_clone = span.clone();
        let fut = self.service.call(req);
        Box::pin(async move {
            let result = fut.instrument(span_clone).await;
            match &result {
                Ok(res) => {
                    span.record("status", res.status().as_u16());
                }
                Err(_) => {
                    span.record("status", 500i64);
                }
            }
            result
        })
    }
}
