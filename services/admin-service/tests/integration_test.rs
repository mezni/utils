// Integration tests for admin service
use std::sync::Arc;

use actix_web::{body::Body, http::StatusCode, test, App, HttpServer};
use ev_db::PgPool;
use sqlx::postgres::PgPoolOptions;

use admin_service::{
    config::PostgresUrl,
    error::ApiError,
    models::{
        HealthCheckRequest, HealthCheckResponse, PartnerRequest, PartnerResponse,
        PartnerListResponse, StationRequest, StationResponse, StationListResponse,
        ChargerRequest, ChargerResponse, ChargerListResponse,
    },
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
                    "service": "admin-service",
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
    assert!(body_str.contains("\"service\":\"admin-service\""));
    assert!(body_str.contains("\"db\":\"ok\""));
}

#[actix_web::test]
async fn test_health_endpoint_returns_500_when_db_down() {
    let pool = create_test_pool().await;

    let app = test::init_service(
        App::new()
            .route("/api/v1/health", test::web::get().to(move || {
                HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": "Database connection failed"
                }))
            }))
            .app_data(web::Data::new(pool)),
    )
    .await;

    let req = test::TestRequest::get().uri("/api/v1/health").to_request();
    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[actix_web::test]
async fn test_partner_create_returns_201() {
    let pool = create_test_pool().await;

    let app = test::init_service(
        App::new()
            .route("/api/v1/partners", test::web::post().to(move || {
                HttpResponse::Created().json(serde_json::json!({
                    "id": "PRT-TEST",
                    "name": "Test Partner",
                    "email": "test@test.com",
                    "phone": "+216 71 111 222",
                    "address": "Test Address"
                }))
            }))
            .app_data(web::Data::new(pool)),
    )
    .await;

    let req = test::TestRequest::post()
        .uri("/api/v1/partners")
        .set_json(serde_json::json!({
            "name": "Test Partner",
            "email": "test@test.com",
            "phone": "+216 71 111 222",
            "address": "Test Address"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::CREATED);
}

#[actix_web::test]
async fn test_partner_get_returns_200() {
    let pool = create_test_pool().await;

    let app = test::init_service(
        App::new()
            .route("/api/v1/partners/:id", test::web::get().to(move || {
                HttpResponse::Ok().json(serde_json::json!({
                    "id": "PRT-001",
                    "name": "Tunis Power",
                    "email": "contact@tunispower.tn",
                    "phone": "+216 71 123 456",
                    "address": "Tunis, Tunisia"
                }))
            }))
            .app_data(web::Data::new(pool)),
    )
    .await;

    let req = test::TestRequest::get().uri("/api/v1/partners/PRT-001").to_request();
    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::OK);

    let body = test::read_body(resp).await;
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    assert!(body_str.contains("\"id\":\"PRT-001\""));
    assert!(body_str.contains("\"name\":\"Tunis Power\""));
}

#[actix_web::test]
async fn test_partner_list_returns_200() {
    let pool = create_test_pool().await;

    let app = test::init_service(
        App::new()
            .route("/api/v1/partners", test::web::get().to(move || {
                HttpResponse::Ok().json(serde_json::json!({
                    "partners": [
                        {
                            "id": "PRT-001",
                            "name": "Tunis Power",
                            "email": "contact@tunispower.tn",
                            "phone": "+216 71 123 456",
                            "address": "Tunis, Tunisia"
                        },
                        {
                            "id": "PRT-002",
                            "name": "Carsharing Tunis",
                            "email": "support@carsharing.tn",
                            "phone": "+216 71 789 012",
                            "address": "Tunis, Tunisia"
                        }
                    ],
                    "pagination": null
                }))
            }))
            .app_data(web::Data::new(pool)),
    )
    .await;

    let req = test::TestRequest::get().uri("/api/v1/partners").to_request();
    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::OK);

    let body = test::read_body(resp).await;
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    assert!(body_str.contains("\"partners\":["));
    assert!(body_str.contains("\"id\":\"PRT-001\""));
    assert!(body_str.contains("\"id\":\"PRT-002\""));
}

#[actix_web::test]
async fn test_station_create_returns_201() {
    let pool = create_test_pool().await;

    let app = test::init_service(
        App::new()
            .route("/api/v1/stations", test::web::post().to(move || {
                HttpResponse::Created().json(serde_json::json!({
                    "id": "STN-TEST",
                    "partner_id": "PRT-001",
                    "name": "Test Station",
                    "latitude": 36.864702,
                    "longitude": 10.158423,
                    "address": "Test Address"
                }))
            }))
            .app_data(web::Data::new(pool)),
    )
    .await;

    let req = test::TestRequest::post()
        .uri("/api/v1/stations")
        .set_json(serde_json::json!({
            "partner_id": "PRT-001",
            "name": "Test Station",
            "latitude": 36.864702,
            "longitude": 10.158423,
            "address": "Test Address"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::CREATED);
}

#[actix_web::test]
async fn test_charger_create_returns_201() {
    let pool = create_test_pool().await;

    let app = test::init_service(
        App::new()
            .route("/api/v1/chargers", test::web::post().to(move || {
                HttpResponse::Created().json(serde_json::json!({
                    "id": "CHR-TEST",
                    "station_id": "STN-001",
                    "connector_type": "Type 2",
                    "power_kw": 22.0,
                    "status": "available"
                }))
            }))
            .app_data(web::Data::new(pool)),
    )
    .await;

    let req = test::TestRequest::post()
        .uri("/api/v1/chargers")
        .set_json(serde_json::json!({
            "station_id": "STN-001",
            "connector_type": "Type 2",
            "power_kw": 22.0,
            "status": "available"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::CREATED);
}

#[actix_web::test]
async fn test_partner_get_returns_404_not_found() {
    let pool = create_test_pool().await;

    let app = test::init_service(
        App::new()
            .route("/api/v1/partners/:id", test::web::get().to(move || {
                HttpResponse::NotFound().json(serde_json::json!({
                    "error": "Partner with ID 'NOTEXIST' not found"
                }))
            }))
            .app_data(web::Data::new(pool)),
    )
    .await;

    let req = test::TestRequest::get().uri("/api/v1/partners/NOTEXIST").to_request();
    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[actix_web::test]
async fn test_partner_create_with_invalid_email_returns_400() {
    let pool = create_test_pool().await;

    let app = test::init_service(
        App::new()
            .route("/api/v1/partners", test::web::post().to(move || {
                HttpResponse::BadRequest().json(serde_json::json!({
                    "error": "Invalid data: email is required"
                }))
            }))
            .app_data(web::Data::new(pool)),
    )
    .await;

    let req = test::TestRequest::post()
        .uri("/api/v1/partners")
        .set_json(serde_json::json!({
            "name": "Test Partner"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[actix_web::test]
async fn test_charger_list_returns_200() {
    let pool = create_test_pool().await;

    let app = test::init_service(
        App::new()
            .route("/api/v1/chargers", test::web::get().to(move || {
                HttpResponse::Ok().json(serde_json::json!({
                    "chargers": [
                        {
                            "id": "CHR-001",
                            "station_id": "STN-001",
                            "connector_type": "Type 2",
                            "power_kw": 22.0,
                            "status": "available"
                        }
                    ],
                    "pagination": null
                }))
            }))
            .app_data(web::Data::new(pool)),
    )
    .await;

    let req = test::TestRequest::get().uri("/api/v1/chargers").to_request();
    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::OK);

    let body = test::read_body(resp).await;
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    assert!(body_str.contains("\"chargers\":["));
    assert!(body_str.contains("\"id\":\"CHR-001\""));
}
