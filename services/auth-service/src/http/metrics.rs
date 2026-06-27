use actix_web::dev::{ServiceRequest, ServiceResponse, Transform, Service};
use actix_web::{get, web, Error, HttpResponse};
use futures::future::{ok, Ready, LocalBoxFuture};
use prometheus::{
    register_counter_vec_with_registry, register_gauge_with_registry,
    register_histogram_vec_with_registry, CounterVec, Encoder, Gauge, HistogramVec, Registry,
    TextEncoder,
};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

pub struct PrometheusMetrics {
    pub registry: Registry,
    http_requests_total: CounterVec,
    http_request_duration_seconds: HistogramVec,
    http_active_requests: Gauge,
}

impl PrometheusMetrics {
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Registry::new();

        let http_requests_total = register_counter_vec_with_registry!(
            "http_requests_total",
            "Total number of HTTP requests",
            &["method", "path", "status"],
            registry
        )?;

        let http_request_duration_seconds = register_histogram_vec_with_registry!(
            "http_request_duration_seconds",
            "HTTP request latency in seconds",
            &["method", "path"],
            registry
        )?;

        let http_active_requests = register_gauge_with_registry!(
            "http_active_requests",
            "Number of active HTTP requests",
            registry
        )?;

        Ok(Self {
            registry,
            http_requests_total,
            http_request_duration_seconds,
            http_active_requests,
        })
    }

    fn observe(&self, method: &str, path: &str, status: u16, duration_secs: f64) {
        let status_str = status.to_string();
        self.http_requests_total
            .with_label_values(&[method, path, &status_str])
            .inc();
        self.http_request_duration_seconds
            .with_label_values(&[method, path])
            .observe(duration_secs);
    }
}

#[get("/metrics")]
pub async fn metrics_handler(metrics: web::Data<PrometheusMetrics>) -> HttpResponse {
    let encoder = TextEncoder::new();
    let metric_families = metrics.registry.gather();
    let mut buffer = Vec::new();
    if encoder.encode(&metric_families, &mut buffer).is_ok() {
        HttpResponse::Ok()
            .content_type("text/plain; charset=utf-8")
            .body(buffer)
    } else {
        HttpResponse::InternalServerError().finish()
    }
}

#[derive(Clone)]
pub struct MetricsMiddlewareFactory {
    metrics: Arc<PrometheusMetrics>,
}

impl MetricsMiddlewareFactory {
    pub fn new(metrics: Arc<PrometheusMetrics>) -> Self {
        Self { metrics }
    }
}

impl<S> Transform<S, ServiceRequest> for MetricsMiddlewareFactory
where
    S: Service<ServiceRequest, Response = ServiceResponse, Error = Error> + 'static,
    S::Future: 'static,
{
    type Response = ServiceResponse;
    type Error = Error;
    type Transform = MetricsMiddleware<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ok(MetricsMiddleware {
            service: Arc::new(service),
            metrics: self.metrics.clone(),
        })
    }
}

pub struct MetricsMiddleware<S> {
    service: Arc<S>,
    metrics: Arc<PrometheusMetrics>,
}

impl<S> Service<ServiceRequest> for MetricsMiddleware<S>
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
        let path = req.path().to_string();

        if path == "/metrics" {
            return Box::pin(self.service.call(req));
        }

        let metrics = self.metrics.clone();
        let service = self.service.clone();
        let method = req.method().to_string();

        metrics.http_active_requests.inc();

        let start = Instant::now();
        let fut = service.call(req);
        Box::pin(async move {
            let result = fut.await;
            let duration = start.elapsed().as_secs_f64();

            match &result {
                Ok(res) => {
                    let status = res.status().as_u16();
                    metrics.observe(&method, &path, status, duration);
                }
                Err(_) => {
                    metrics.observe(&method, &path, 500, duration);
                }
            }

            metrics.http_active_requests.dec();
            result
        })
    }
}
