use axum::body::Body;
use axum::http::{Request, StatusCode};
use sqlx::PgPool;
use tower::ServiceExt;

use driver_service::build_router;

fn get_test_pool() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    if url.contains("localhost") || url.contains("test") {
        let pool = PgPool::connect_lazy(&url).ok()?;
        Some(pool)
    } else {
        None
    }
}

#[tokio::test]
async fn test_nearby_search_valid() {
    let pool = match get_test_pool() {
        Some(p) => p,
        None => {
            eprintln!("Skipping: DATABASE_URL not set or not a test URL");
            return;
        }
    };

    let app = build_router(pool).await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/stations/nearby?lat=36.78&lng=10.19&radius=50000")
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
    let stations: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();

    // Should find nearby stations in Tunis area
    if !stations.is_empty() {
        // Verify distances are in ascending order
        for i in 1..stations.len() {
            let prev = stations[i - 1]["distance"].as_f64().unwrap_or(f64::MAX);
            let cur = stations[i]["distance"].as_f64().unwrap_or(0.0);
            assert!(
                cur >= prev,
                "Stations not in proximity order: prev={}, cur={}",
                prev,
                cur
            );
        }
    }
}

#[tokio::test]
async fn test_nearby_search_no_results() {
    let pool = match get_test_pool() {
        Some(p) => p,
        None => {
            eprintln!("Skipping: DATABASE_URL not set or not a test URL");
            return;
        }
    };

    let app = build_router(pool).await;

    // Query in the middle of the ocean
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/stations/nearby?lat=0.0&lng=0.0&radius=100")
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
    let stations: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();

    assert!(
        stations.is_empty(),
        "Expected empty result for middle-of-ocean query"
    );
}

#[tokio::test]
async fn test_nearby_search_missing_params() {
    let pool = match get_test_pool() {
        Some(p) => p,
        None => {
            eprintln!("Skipping: DATABASE_URL not set or not a test URL");
            return;
        }
    };

    let app = build_router(pool).await;

    // Missing lat and lng
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/stations/nearby?radius=5000")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_nearby_search_invalid_lat() {
    let pool = match get_test_pool() {
        Some(p) => p,
        None => {
            eprintln!("Skipping: DATABASE_URL not set or not a test URL");
            return;
        }
    };

    let app = build_router(pool).await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/stations/nearby?lat=999&lng=10.0&radius=5000")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();
    assert!(body_str.contains("lat"), "Body: {}", body_str);
}
