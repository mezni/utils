use actix_web::dev::ServiceResponse;
use actix_web::{web, App, HttpResponse, test};
use auth_service::http::metrics::{metrics_handler, MetricsMiddlewareFactory, PrometheusMetrics};
use auth_service::http::middleware::logging::LoggingMiddleware;
use auth_service::http::middleware::request_id::{RequestId, RequestIdMiddleware};
use auth_service::http::middleware::tracing::TracingMiddleware;
use std::sync::Arc;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("off")
        .try_init();
}

#[actix_web::test]
async fn request_id_middleware_adds_header() {
    init_tracing();

    let metrics = Arc::new(PrometheusMetrics::new().unwrap());
    let metrics_mw = MetricsMiddlewareFactory::new(metrics.clone());

    let app = test::init_service(
        App::new()
            .wrap(RequestIdMiddleware)
            .wrap(TracingMiddleware)
            .wrap(metrics_mw)
            .wrap(LoggingMiddleware)
            .route("/test", web::get().to(|| async { HttpResponse::Ok() })),
    )
    .await;

    let req = test::TestRequest::get().uri("/test").to_request();
    let res: ServiceResponse = test::call_service(&app, req).await;

    assert!(res.status().is_success());
    assert!(res.headers().contains_key("X-Request-ID"));

    let request_id = res
        .headers()
        .get("X-Request-ID")
        .and_then(|v| v.to_str().ok())
        .unwrap();
    assert!(!request_id.is_empty());
}

#[actix_web::test]
async fn request_id_preserves_incoming_header() {
    init_tracing();

    let metrics = Arc::new(PrometheusMetrics::new().unwrap());
    let metrics_mw = MetricsMiddlewareFactory::new(metrics.clone());

    let app = test::init_service(
        App::new()
            .wrap(RequestIdMiddleware)
            .wrap(TracingMiddleware)
            .wrap(metrics_mw)
            .wrap(LoggingMiddleware)
            .route("/test", web::get().to(|| async { HttpResponse::Ok() })),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/test")
        .insert_header(("X-Request-ID", "custom-id-123"))
        .to_request();
    let res: ServiceResponse = test::call_service(&app, req).await;

    assert!(res.status().is_success());
    let request_id = res
        .headers()
        .get("X-Request-ID")
        .and_then(|v| v.to_str().ok())
        .unwrap();
    assert_eq!(request_id, "custom-id-123");
}

#[actix_web::test]
async fn request_id_extractor_works_with_middleware() {
    init_tracing();

    let metrics = Arc::new(PrometheusMetrics::new().unwrap());
    let metrics_mw = MetricsMiddlewareFactory::new(metrics.clone());

    let app = test::init_service(
        App::new()
            .app_data(web::Data::from(metrics.clone()))
            .wrap(RequestIdMiddleware)
            .wrap(TracingMiddleware)
            .wrap(metrics_mw)
            .wrap(LoggingMiddleware)
            .route(
                "/test",
                web::get().to(|request_id: RequestId| async move {
                    HttpResponse::Ok()
                        .insert_header(("X-Echo-ID", request_id.as_str()))
                        .finish()
                }),
            ),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/test")
        .insert_header(("X-Request-ID", "echo-test"))
        .to_request();
    let res: ServiceResponse = test::call_service(&app, req).await;

    assert!(res.status().is_success());
    let echoed = res
        .headers()
        .get("X-Echo-ID")
        .and_then(|v| v.to_str().ok())
        .unwrap();
    assert_eq!(echoed, "echo-test");
}

#[actix_web::test]
async fn metrics_endpoint_returns_prometheus_format() {
    init_tracing();

    let metrics = Arc::new(PrometheusMetrics::new().unwrap());

    let app = test::init_service(
        App::new()
            .app_data(web::Data::from(metrics.clone()))
            .service(metrics_handler),
    )
    .await;

    let req = test::TestRequest::get().uri("/metrics").to_request();
    let res: ServiceResponse = test::call_service(&app, req).await;

    assert!(res.status().is_success());
    let content_type = res
        .headers()
        .get("Content-Type")
        .and_then(|v| v.to_str().ok())
        .unwrap();
    assert!(content_type.contains("text/plain"));
}

#[actix_web::test]
async fn metrics_endpoint_exposes_counters() {
    init_tracing();

    let metrics = Arc::new(PrometheusMetrics::new().unwrap());
    let metrics_mw = MetricsMiddlewareFactory::new(metrics.clone());

    let app = test::init_service(
        App::new()
            .app_data(web::Data::from(metrics.clone()))
            .wrap(RequestIdMiddleware)
            .wrap(metrics_mw)
            .service(metrics_handler)
            .route("/api/test", web::get().to(|| async { HttpResponse::Ok() })),
    )
    .await;

    let req = test::TestRequest::get().uri("/api/test").to_request();
    let _ = test::call_service(&app, req).await;

    let req = test::TestRequest::get().uri("/metrics").to_request();
    let res: ServiceResponse = test::call_service(&app, req).await;

    let body = test::read_body(res).await;
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    assert!(
        body_str.contains("http_requests_total"),
        "metrics should contain http_requests_total, got: {}",
        body_str
    );
    assert!(
        body_str.contains("http_request_duration_seconds"),
        "metrics should contain http_request_duration_seconds, got: {}",
        body_str
    );
    assert!(
        body_str.contains("http_active_requests"),
        "metrics should contain http_active_requests, got: {}",
        body_str
    );
}

#[actix_web::test]
async fn metrics_middleware_does_not_modify_metrics_on_self() {
    init_tracing();

    let metrics = Arc::new(PrometheusMetrics::new().unwrap());
    let metrics_mw = MetricsMiddlewareFactory::new(metrics.clone());

    let app = test::init_service(
        App::new()
            .app_data(web::Data::from(metrics.clone()))
            .wrap(RequestIdMiddleware)
            .wrap(metrics_mw)
            .service(metrics_handler)
            .route("/other", web::get().to(|| async { HttpResponse::Ok() })),
    )
    .await;

    let req = test::TestRequest::get().uri("/other").to_request();
    let _ = test::call_service(&app, req).await;

    let req = test::TestRequest::get().uri("/metrics").to_request();
    let res: ServiceResponse = test::call_service(&app, req).await;

    let body = test::read_body(res).await;
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    assert!(body_str.contains(r#"http_requests_total{method="GET",path="/other",status="200"}"#));

    let metrics_line_count = body_str.lines()
        .filter(|l| l.starts_with("http_requests_total{method=\"GET\",path=\"/metrics\""))
        .count();
    assert_eq!(metrics_line_count, 0, "/metrics must never modify counters");
}

#[actix_web::test]
async fn middleware_pipeline_produces_unique_request_ids() {
    init_tracing();

    let metrics = Arc::new(PrometheusMetrics::new().unwrap());
    let metrics_mw = MetricsMiddlewareFactory::new(metrics.clone());

    let app = test::init_service(
        App::new()
            .wrap(RequestIdMiddleware)
            .wrap(TracingMiddleware)
            .wrap(metrics_mw)
            .wrap(LoggingMiddleware)
            .route("/a", web::get().to(|| async { HttpResponse::Ok() }))
            .route("/b", web::get().to(|| async { HttpResponse::Ok() })),
    )
    .await;

    let req_a = test::TestRequest::get().uri("/a").to_request();
    let res_a: ServiceResponse = test::call_service(&app, req_a).await;
    let id_a = res_a
        .headers()
        .get("X-Request-ID")
        .and_then(|v| v.to_str().ok())
        .unwrap()
        .to_string();

    let req_b = test::TestRequest::get().uri("/b").to_request();
    let res_b: ServiceResponse = test::call_service(&app, req_b).await;
    let id_b = res_b
        .headers()
        .get("X-Request-ID")
        .and_then(|v| v.to_str().ok())
        .unwrap()
        .to_string();

    assert_ne!(id_a, id_b, "each request must get a unique request ID");
}

#[actix_web::test]
async fn metrics_active_requests_gauge_functions() {
    init_tracing();

    let metrics = Arc::new(PrometheusMetrics::new().unwrap());

    let app = test::init_service(
        App::new()
            .app_data(web::Data::from(metrics.clone()))
            .service(metrics_handler),
    )
    .await;

    let req = test::TestRequest::get().uri("/metrics").to_request();
    let res: ServiceResponse = test::call_service(&app, req).await;

    let body = test::read_body(res).await;
    let body_str = String::from_utf8(body.to_vec()).unwrap();
    assert!(body_str.contains("http_active_requests"));
}
