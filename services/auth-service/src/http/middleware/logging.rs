use actix_web::dev::{ServiceRequest, ServiceResponse, Transform, Service};
use actix_web::Error;
use futures::future::{ok, Ready, LocalBoxFuture};
use std::task::{Context, Poll};
use std::time::Instant;
use tracing::info;

use super::request_id;

pub struct LoggingMiddleware;

impl<S> Transform<S, ServiceRequest> for LoggingMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse, Error = Error> + 'static,
    S::Future: 'static,
{
    type Response = ServiceResponse;
    type Error = Error;
    type Transform = LoggingMiddlewareService<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ok(LoggingMiddlewareService { service })
    }
}

pub struct LoggingMiddlewareService<S> {
    service: S,
}

impl<S> Service<ServiceRequest> for LoggingMiddlewareService<S>
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
        let start = Instant::now();
        let request_id = request_id::get_request_id(&req);
        let method = req.method().to_string();
        let path = req.path().to_string();
        let client_ip = req
            .peer_addr()
            .map(|addr| addr.ip().to_string())
            .unwrap_or_default();

        let fut = self.service.call(req);
        Box::pin(async move {
            let result = fut.await;
            let duration_ms = start.elapsed().as_secs_f64() * 1000.0;

            match &result {
                Ok(res) => {
                    let status = res.status().as_u16();
                    info!(
                        request_id = %request_id,
                        method = %method,
                        path = %path,
                        status = status,
                        duration_ms = format!("{:.2}", duration_ms),
                        service = "auth-service",
                        client_ip = %client_ip,
                        "request completed"
                    );
                }
                Err(err) => {
                    info!(
                        request_id = %request_id,
                        method = %method,
                        path = %path,
                        status = 500i64,
                        duration_ms = format!("{:.2}", duration_ms),
                        service = "auth-service",
                        client_ip = %client_ip,
                        error = %err,
                        "request failed"
                    );
                }
            }
            result
        })
    }
}
