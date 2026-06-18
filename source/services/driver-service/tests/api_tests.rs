use actix_web::{test, web, App};
use sqlx::PgPool;

async fn get_pool() -> PgPool {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://bornemap:bornemap_dev@localhost:5432/platform_db".into());
    PgPool::connect(&url).await.unwrap()
}

#[actix_web::test]
async fn test_nearby_valid() {
    let pool = get_pool().await;
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .configure(driver_service::api::configure),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/nearby?lat=36.8065&lng=10.1815&radius=50000")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success(), "Expected 2xx, got {}", resp.status());

    let body: serde_json::Value = test::read_body_json(resp).await;
    assert!(body.get("stations").is_some(), "Response should have 'stations' field");
}

#[actix_web::test]
async fn test_nearby_empty_result() {
    let pool = get_pool().await;
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .configure(driver_service::api::configure),
    )
    .await;

    // Use coordinates in the middle of the Atlantic Ocean (far from our Tunisian stations)
    let req = test::TestRequest::get()
        .uri("/api/v1/nearby?lat=0&lng=-30&radius=10000")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success(), "Expected 2xx, got {}", resp.status());

    let body: serde_json::Value = test::read_body_json(resp).await;
    let stations = body.get("stations").and_then(|s| s.as_array());
    assert!(stations.is_some(), "Response should have 'stations' array");
    assert!(stations.unwrap().is_empty(), "Expected empty stations array");
}

#[actix_web::test]
async fn test_nearby_invalid_lat() {
    let pool = get_pool().await;
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .configure(driver_service::api::configure),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/nearby?lat=100&lng=10&radius=10000")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 400, "Expected 400 for invalid latitude");
}

#[actix_web::test]
async fn test_health() {
    let pool = get_pool().await;
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .configure(driver_service::api::configure),
    )
    .await;

    let req = test::TestRequest::get().uri("/health").to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success(), "Expected 2xx, got {}", resp.status());

    let body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(body.get("status").and_then(|s| s.as_str()), Some("ok"));
}
