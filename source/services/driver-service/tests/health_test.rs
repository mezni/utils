use axum::body::Body;
use axum::http::{Request, StatusCode};
use sqlx::PgPool;
use tower::ServiceExt;

use driver_service::build_router;

#[tokio::test]
async fn test_health_check_with_invalid_db() {
    let pool = PgPool::connect_lazy("postgres://invalid:invalid@localhost:5432/test")
        .expect("Lazy pool creation should always succeed");

    let app = build_router(pool).await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/health")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    assert!(body_str.contains("disconnected"), "Body: {}", body_str);
}
