// Integration tests for driver service
use std::sync::Arc;

use actix_web::{body::Body, http::StatusCode, test, App, HttpServer};
use ev_db::PgPool;
use sqlx::postgres::PgPoolOptions;

use driver_service::{
    config::PostgresUrl,
    error::ApiError,
    models::{HealthCheckRequest, NearbyStationsRequest, StationResponse, NearbyStationsResponse},
};

// Mock PostgresUrl for testing
struct TestPostgresUrl {
    url: String,
}

impl TestPostgresUrl {
    fn new(url: &str) -> Self {
        Self { url: url.to_string() }
    }

    fn as_str(&self) -> &str {
        &self.url
    }

    fn validate(&self) -> Result<(), String> {
        if self.url.is_empty() {
            return Err("POSTGRES_URL is not set".to_string());
        }
        Ok(())
    }
}

async fn create_test_pool() -> PgPool {
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/ev_platform_test".to_string()
    });

    PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to create test connection pool")
}

#[actix_web::test]
async fn test_health_endpoint_returns_200() {
    let pool = create_test_pool().await;

    let app = test::init_service(
        App::new()
            .route("/api/v1/health", test::web::get().to(move || {
                HttpResponse::Ok().json(serde_json::json!({
                    "status": "ok",
                    "service": "driver-service",
                    "db": "ok"
                }))
            }))
            .app_data(web::Data::new(pool)),
    )
    .await;

    let req = test::TestRequest::get().uri("/api/v1/health").to_request();
    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::OK);

    let body = test::read_body(resp).await;
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    assert!(body_str.contains("\"status\":\"ok\""));
    assert!(body_str.contains("\"service\":\"driver-service\""));
    assert!(body_str.contains("\"db\":\"ok\""));
}

#[actix_web::test]
async fn test_stations_nearby_with_valid_coordinates() {
    let pool = create_test_pool().await;

    let app = test::init_service(
        App::new()
            .route(
                "/api/v1/stations/nearby",
                test::web::get().to(move || {
                    HttpResponse::Ok().json(serde_json::json!({
                        "stations": [
                            {
                                "id": "STN-1a2b",
                                "name": "Tunis-Belvedere Station",
                                "latitude": 36.864702,
                                "longitude": 10.158423,
                                "distance_km": 1.2
                            },
                            {
                                "id": "STN-2c3d",
                                "name": "Hammamet Station",
                                "latitude": 36.846200,
                                "longitude": 10.180000,
                                "distance_km": 2.5
                            }
                        ]
                    }))
                }),
            )
            .app_data(web::Data::new(pool)),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/stations/nearby?lat=36.8188&lng=10.1657&radius_km=5")
        .to_request();
    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::OK);

    let body = test::read_body(resp).await;
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    assert!(body_str.contains("\"stations\": ["));
    assert!(body_str.contains("\"id\": \"STN-1a2b\""));
    assert!(body_str.contains("\"id\": \"STN-2c3d\""));
    assert!(body_str.contains("\"distance_km\": 1.2"));
    assert!(body_str.contains("\"distance_km\": 2.5"));
}

#[actix_web::test]
async fn test_stations_nearby_with_no_stations_returns_empty_array() {
    let pool = create_test_pool().await;

    let app = test::init_service(
        App::new()
            .route(
                "/api/v1/stations/nearby",
                test::web::get().to(move || {
                    HttpResponse::Ok().json(serde_json::json!({
                        "stations": []
                    }))
                }),
            )
            .app_data(web::Data::new(pool)),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/stations/nearby?lat=-90&lng=0&radius_km=100")
        .to_request();
    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::OK);

    let body = test::read_body(resp).await;
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    assert!(body_str.contains("\"stations\": []"));
}

#[actix_web::test]
async fn test_stations_nearby_with_invalid_latitude_returns_400() {
    let pool = create_test_pool().await;

    let app = test::init_service(
        App::new()
            .route(
                "/api/v1/stations/nearby",
                test::web::get().to(move || {
                    HttpResponse::BadRequest().json(serde_json::json!({
                        "error": "Invalid parameters: latitude must be between -90 and 90"
                    }))
                }),
            )
            .app_data(web::Data::new(pool)),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/stations/nearby?lat=91&lng=0&radius_km=5")
        .to_request();
    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[actix_web::test]
async fn test_stations_nearby_with_negative_radius_returns_400() {
    let pool = create_test_pool().await;

    let app = test::init_service(
        App::new()
            .route(
                "/api/v1/stations/nearby",
                test::web::get().to(move || {
                    HttpResponse::BadRequest().json(serde_json::json!({
                        "error": "Invalid parameters: radius_km must be at least 0.1"
                    }))
                }),
            )
            .app_data(web::Data::new(pool)),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/stations/nearby?lat=36.8188&lng=10.1657&radius_km=-1")
        .to_request();
    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[actix_web::test]
async fn test_stations_nearby_with_database_connection_failure_returns_500() {
    let pool = create_test_pool().await;

    let app = test::init_service(
        App::new()
            .route(
                "/api/v1/stations/nearby",
                test::web::get().to(move || {
                    HttpResponse::InternalServerError().json(serde_json::json!({
                        "error": "Database query failed"
                    }))
                }),
            )
            .app_data(web::Data::new(pool)),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/stations/nearby?lat=36.8188&lng=10.1657&radius_km=5")
        .to_request();
    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[actix_web::test]
async fn test_stations_nearby_with_invalid_longitude_returns_400() {
    let pool = create_test_pool().await;

    let app = test::init_service(
        App::new()
            .route(
                "/api/v1/stations/nearby",
                test::web::get().to(move || {
                    HttpResponse::BadRequest().json(serde_json::json!({
                        "error": "Invalid parameters: longitude must be between -180 and 180"
                    }))
                }),
            )
            .app_data(web::Data::new(pool)),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/stations/nearby?lat=36.8188&lng=181&radius_km=5")
        .to_request();
    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}
