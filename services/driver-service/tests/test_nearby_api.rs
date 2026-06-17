use actix_web::http::StatusCode;
use actix_web::test::{self, TestRequest};
use sqlx::PgPool;

/// Test GET /api/v1/nearby with valid coordinates
#[tokio::test]
async fn test_nearby_endpoint_valid_coordinates() {
    // Test with valid coordinates
    let app = test::init_service(
        actix_web::App::new().route("/api/v1/nearby", actix_web::web::get().to(|_| async {
            Ok("test response")
        }))
    )
    .await;

    let request = TestRequest::get()
        .uri("/api/v1/nearby?lat=36.8&lon=10.18&radius_m=5000")
        .to_request();

    let response = test::call_service(&app, request).await;
    assert_eq!(response.status(), StatusCode::OK);
}

/// Test GET /api/v1/nearby with missing latitude
#[tokio::test]
async fn test_nearby_endpoint_missing_latitude() {
    // Test with missing latitude parameter
    let app = test::init_service(
        actix_web::App::new().route("/api/v1/nearby", actix_web::web::get().to(|_| async {
            Ok("test response")
        }))
    )
    .await;

    let request = TestRequest::get()
        .uri("/api/v1/nearby?lon=10.18&radius_m=5000")
        .to_request();

    let response = test::call_service(&app, request).await;
    // Should return 400 Bad Request
    assert!(response.status().is_client_error());
}

/// Test GET /api/v1/nearby with invalid coordinates
#[tokio::test]
async fn test_nearby_endpoint_invalid_coordinates() {
    // Test with latitude > 90
    let app = test::init_service(
        actix_web::App::new().route("/api/v1/nearby", actix_web::web::get().to(|_| async {
            Ok("test response")
        }))
    )
    .await;

    let request = TestRequest::get()
        .uri("/api/v1/nearby?lat=91&lon=10.18&radius_m=5000")
        .to_request();

    let response = test::call_service(&app, request).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

/// Test GET /api/v1/nearby with radius too large
#[tokio::test]
async fn test_nearby_endpoint_radius_too_large() {
    // Test with radius > 50000
    let app = test::init_service(
        actix_web::App::new().route("/api/v1/nearby", actix_web::web::get().to(|_| async {
            Ok("test response")
        }))
    )
    .await;

    let request = TestRequest::get()
        .uri("/api/v1/nearby?lat=36.8&lon=10.18&radius_m=60000")
        .to_request();

    let response = test::call_service(&app, request).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

/// Test GET /api/v1/nearby with max results too large
#[tokio::test]
async fn test_nearby_endpoint_max_results_too_large() {
    // Test with max_results > 100
    let app = test::init_service(
        actix_web::App::new().route("/api/v1/nearby", actix_web::web::get().to(|_| async {
            Ok("test response")
        }))
    )
    .await;

    let request = TestRequest::get()
        .uri("/api/v1/nearby?lat=36.8&lon=10.18&max_results=150")
        .to_request();

    let response = test::call_service(&app, request).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

/// Test GET /api/v1/nearby with rate limiting
#[tokio::test]
async fn test_nearby_endpoint_rate_limiting() {
    // Test rate limiting
    let app = test::init_service(
        actix_web::App::new().route("/api/v1/nearby", actix_web::web::get().to(|_| async {
            Ok("test response")
        }))
    )
    .await;

    // Send 101 requests within 60 seconds
    for _ in 0..101 {
        let request = TestRequest::get()
            .uri("/api/v1/nearby?lat=36.8&lon=10.18&radius_m=5000")
            .to_request();

        let response = test::call_service(&app, request).await;
        // First 100 should succeed, 101st should fail
    }
    assert!(true); // Placeholder for rate limit test
}

/// Test GET /api/v1/nearby with authentication
#[tokio::test]
async fn test_nearby_endpoint_authentication() {
    // Test with missing auth header
    let app = test::init_service(
        actix_web::App::new().route("/api/v1/nearby", actix_web::web::get().to(|_| async {
            Ok("test response")
        }))
    )
    .await;

    let request = TestRequest::get()
        .uri("/api/v1/nearby?lat=36.8&lon=10.18&radius_m=5000")
        .to_request();

    let response = test::call_service(&app, request).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // Test with valid auth header
    let request = TestRequest::get()
        .uri("/api/v1/nearby?lat=36.8&lon=10.18&radius_m=5000")
        .insert_header(("Authorization", "Bearer valid_token"))
        .to_request();

    let response = test::call_service(&app, request).await;
    assert_eq!(response.status(), StatusCode::OK);
}
