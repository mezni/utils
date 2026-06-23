//! Integration tests for analytics API endpoints
//! Tests for User Story 1: Admin Dashboard

use actix_web::body::Body;
use actix_web::test::{self, TestRequest};
use actix_web::{http, web, App};
use std::sync::Arc;

use admin_service::api::analytics::{AppState, get_cache_health, get_search_trends, get_summary, get_station_analytics, get_user_activity};
use admin_service::services::CacheService;

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a mock AppState for testing
    fn create_mock_state() -> Arc<AppState> {
        // In a real test, you would create proper connections
        // For now, we'll use a mock
        let cache_service = Arc::new(CacheService::new(
            admin_service::services::CacheConfig {
                redis_url: "redis://127.0.0.1:6379".to_string(),
                default_ttl_seconds: 60,
                max_connections: 10,
            },
        ));

        Arc::new(AppState {
            kpi_engine: Arc::new(admin_service::services::KPIAggregationEngine::new(
                admin_service::services::KPIConfig::default(),
                // Mock pool
                web::Data::new(()).into_inner(),
                cache_service,
            )),
            db_pool: Arc::new(web::Data::new(()).into_inner()),
            cache_service,
        })
    }

    #[tokio::test]
    async fn test_get_summary_endpoint() {
        let app_state = create_mock_state();

        let mut app = test::init_service(
            test::web::App::new()
                .app_data(web::Data::new(app_state.clone()))
                .service(web::scope("/api/v1/analytics")
                    .route("/summary", web::get().to(get_summary))
                )
        ).await;

        let req = TestRequest::get()
            .uri("/api/v1/analytics/summary")
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
        assert_eq!(resp.status(), http::StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_summary_with_date_range() {
        let app_state = create_mock_state();

        let mut app = test::init_service(
            test::web::App::new()
                .app_data(web::Data::new(app_state.clone()))
                .service(web::scope("/api/v1/analytics")
                    .route("/summary", web::get().to(get_summary))
                )
        ).await;

        let req = TestRequest::get()
            .uri("/api/v1/analytics/summary?start_date=2026-01-01&end_date=2026-12-31")
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
    }

    #[tokio::test]
    async fn test_get_summary_with_partner_filter() {
        let app_state = create_mock_state();

        let mut app = test::init_service(
            test::web::App::new()
                .app_data(web::Data::new(app_state.clone()))
                .service(web::scope("/api/v1/analytics")
                    .route("/summary", web::get().to(get_summary))
                )
        ).await;

        let req = TestRequest::get()
            .uri("/api/v1/analytics/summary?partner_id=STX-abc123def456")
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
    }

    #[tokio::test]
    async fn test_get_station_analytics_endpoint() {
        let app_state = create_mock_state();

        let mut app = test::init_service(
            test::web::App::new()
                .app_data(web::Data::new(app_state.clone()))
                .service(web::scope("/api/v1/analytics")
                    .route("/stations/:id", web::get().to(get_station_analytics))
                )
        ).await;

        let req = TestRequest::get()
            .uri("/api/v1/analytics/stations/STA-123456")
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        // Note: This might return 404 if station doesn't exist
        // We're just checking the endpoint is accessible
        assert!(resp.status().is_success() || resp.status() == http::StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_get_station_analytics_with_filter() {
        let app_state = create_mock_state();

        let mut app = test::init_service(
            test::web::App::new()
                .app_data(web::Data::new(app_state.clone()))
                .service(web::scope("/api/v1/analytics")
                    .route("/stations/:id", web::get().to(get_station_analytics))
                )
        ).await;

        let req = TestRequest::get()
            .uri("/api/v1/analytics/stations/STA-123456?filter_by_partner=true&partner_id=STX-abc123def456")
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
    }

    #[tokio::test]
    async fn test_get_user_activity_endpoint() {
        let app_state = create_mock_state();

        let mut app = test::init_service(
            test::web::App::new()
                .app_data(web::Data::new(app_state.clone()))
                .service(web::scope("/api/v1/analytics")
                    .route("/users/:uuid", web::get().to(get_user_activity))
                )
        ).await;

        let req = TestRequest::get()
            .uri("/api/v1/analytics/users/550e8400-e29b-41d4-a716-446655440000")
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success() || resp.status() == http::StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_get_user_activity_with_invalid_uuid() {
        let app_state = create_mock_state();

        let mut app = test::init_service(
            test::web::App::new()
                .app_data(web::Data::new(app_state.clone()))
                .service(web::scope("/api/v1/analytics")
                    .route("/users/:uuid", web::get().to(get_user_activity))
                )
        ).await;

        let req = TestRequest::get()
            .uri("/api/v1/analytics/users/invalid-uuid")
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), http::StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_get_search_trends_endpoint() {
        let app_state = create_mock_state();

        let mut app = test::init_service(
            test::web::App::new()
                .app_data(web::Data::new(app_state.clone()))
                .service(web::scope("/api/v1/analytics")
                    .route("/search-trends", web::get().to(get_search_trends))
                )
        ).await;

        let req = TestRequest::get()
            .uri("/api/v1/analytics/search-trends")
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
    }

    #[tokio::test]
    async fn test_get_search_trends_with_date_range() {
        let app_state = create_mock_state();

        let mut app = test::init_service(
            test::web::App::new()
                .app_data(web::Data::new(app_state.clone()))
                .service(web::scope("/api/v1/analytics")
                    .route("/search-trends", web::get().to(get_search_trends))
                )
        ).await;

        let req = TestRequest::get()
            .uri("/api/v1/analytics/search-trends?start_date=2026-01-01&end_date=2026-12-31")
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
    }

    #[tokio::test]
    async fn test_get_search_trends_with_partner_filter() {
        let app_state = create_mock_state();

        let mut app = test::init_service(
            test::web::App::new()
                .app_data(web::Data::new(app_state.clone()))
                .service(web::scope("/api/v1/analytics")
                    .route("/search-trends", web::get().to(get_search_trends))
                )
        ).await;

        let req = TestRequest::get()
            .uri("/api/v1/analytics/search-trends?partner_id=STX-abc123def456")
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
    }

    #[tokio::test]
    async fn test_get_cache_health_endpoint() {
        let app_state = create_mock_state();

        let mut app = test::init_service(
            test::web::App::new()
                .app_data(web::Data::new(app_state.clone()))
                .service(web::scope("/api/v1/analytics")
                    .route("/cache/health", web::get().to(get_cache_health))
                )
        ).await;

        let req = TestRequest::get()
            .uri("/api/v1/analytics/cache/health")
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
    }

    #[test]
    fn test_cache_service_basic_operations() {
        let cache_config = admin_service::services::CacheConfig {
            redis_url: "redis://127.0.0.1:6379".to_string(),
            default_ttl_seconds: 60,
            max_connections: 10,
        };

        let cache_service = CacheService::new(cache_config).expect("Failed to create cache service");

        // Test set and get
        let test_key = "test_key_integration";
        let test_value = "test_value_123";
        cache_service.set(test_key, test_value, Some(60)).expect("Failed to set value");

        let retrieved: String = cache_service.get(test_key).expect("Failed to get value");

        assert_eq!(retrieved, test_value);

        // Test delete
        cache_service.delete(test_key).expect("Failed to delete key");
        let result = cache_service.get::<String>(test_key).expect("Should not find deleted key");
        assert!(result.is_none());
    }
}
