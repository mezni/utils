use actix_web::{
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
    Error, HttpMessage,
};
use futures::future::LocalBoxFuture;
use std::future::{ready, Ready};

/// Middleware that tracks which path prefix was stripped by the gateway.
/// This allows handlers to construct correct response URLs without knowledge of the routing configuration.
pub struct GatewayAwareMiddleware;

impl<S, B> Transform<S, ServiceRequest> for GatewayAwareMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = GatewayAwareMiddlewareService<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(GatewayAwareMiddlewareService { service }))
    }
}

pub struct GatewayAwareMiddlewareService<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for GatewayAwareMiddlewareService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        // Store the original request path for reference in handlers
        let original_path = req.path().to_string();
        req.extensions_mut()
            .insert("x-original-path".to_string(), original_path);

        let fut = self.service.call(req);

        Box::pin(async move {
            let res = fut.await?;
            Ok(res)
        })
    }
}
