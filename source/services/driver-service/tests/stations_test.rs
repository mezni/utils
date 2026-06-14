use axum::body::Body;
use axum::http::{Request, StatusCode};
use sqlx::PgPool;
use tower::ServiceExt;

use driver_service::build_router;

fn get_test_pool() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    // Only use if it points to a real test DB
    if url.contains("localhost") || url.contains("test") {
        let pool = PgPool::connect_lazy(&url).ok()?;
        Some(pool)
    } else {
        None
    }
}

#[tokio::test]
async fn test_list_stations() {
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
                .uri("/api/v1/stations")
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

    assert!(!stations.is_empty(), "Expected at least one station");

    let first = &stations[0];
    assert!(first.get("id").is_some(), "Missing 'id' field");
    assert!(first.get("name").is_some(), "Missing 'name' field");
    assert!(first.get("status").is_some(), "Missing 'status' field");
    assert!(first.get("latitude").is_some(), "Missing 'latitude' field");
    assert!(first.get("longitude").is_some(), "Missing 'longitude' field");
    assert!(first.get("distance").is_some(), "Missing 'distance' field");
}

#[tokio::test]
async fn test_get_station_by_id() {
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
                .uri("/api/v1/stations/STA-00001")
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
    let station: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(station["id"], "STA-00001");
}

#[tokio::test]
async fn test_get_station_not_found() {
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
                .uri("/api/v1/stations/NONEXISTENT")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();
    assert!(body_str.contains("not found"), "Body: {}", body_str);
}
