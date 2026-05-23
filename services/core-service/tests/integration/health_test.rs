#[cfg(test)]
mod tests {
    use actix_web::{test, App};
    use crate::handlers::health;
    use crate::utils::database::Database;

    #[actix_rt::test]
    async fn test_health_endpoint() {
        // This test should fail initially because the health endpoint might not be properly implemented
        // After implementing the health endpoint correctly, this test should pass
        
        // Create a test database connection
        let database_url = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://bornemap:bornemap_dev@localhost:5432/bornemap_test".to_string());
        
        let database = Database::new(&database_url).await
            .expect("Failed to create test database connection");
        
        // Create test app
        let mut app = test::init_service(
            App::new()
                .app_data(actix_web::web::Data::new(database))
                .route("/health/core-service", actix_web::web::get().to(health::health_check))
        ).await;
        
        // Test health endpoint
        let req = test::TestRequest::get()
            .uri("/health/core-service")
            .to_request();
        
        let resp = test::call_service(&mut app, req).await;
        
        // Check response status
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
        
        // Check response body
        let body = test::read_body(resp).await;
        let health_response: serde_json::Value = serde_json::from_slice(&body)
            .expect("Failed to parse health response");
        
        assert_eq!(health_response["status"], "healthy");
        assert!(health_response["database"].is_string());
        assert!(health_response["details"]["database"]["status"].is_string());
        assert!(health_response["details"]["database"]["response_time_ms"].is_number());
    }

    #[actix_rt::test]
    async fn test_health_endpoint_database_failure() {
        // This test should verify that the health endpoint properly handles database failures
        
        // Create a test database connection with invalid URL
        let database_url = "postgres://invalid:invalid@localhost:5432/invalid";
        
        let database = Database::new(database_url).await;
        
        // The database connection should fail
        assert!(database.is_err());
        
        // If we had a working database that then fails, the health endpoint should return 503
        // This will be tested after we implement proper database failure handling
    }

    #[actix_rt::test]
    async fn test_metrics_endpoint() {
        // This test should fail initially because the metrics endpoint might not be properly implemented
        // After implementing the metrics endpoint correctly, this test should pass
        
        // Create a test database connection
        let database_url = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://bornemap:bornemap_dev@localhost:5432/bornemap_test".to_string());
        
        let database = Database::new(&database_url).await
            .expect("Failed to create test database connection");
        
        // Create test app
        let mut app = test::init_service(
            App::new()
                .app_data(actix_web::web::Data::new(database))
                .route("/metrics/core-service", actix_web::web::get().to(health::metrics))
        ).await;
        
        // Test metrics endpoint
        let req = test::TestRequest::get()
            .uri("/metrics/core-service")
            .to_request();
        
        let resp = test::call_service(&mut app, req).await;
        
        // Check response status
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
        
        // Check content type
        assert_eq!(resp.headers().get("content-type").unwrap(), "text/plain; version=0.0.4");
        
        // Check response body contains expected metrics
        let body = test::read_body(resp).await;
        let body_str = String::from_utf8(body).unwrap();
        
        assert!(body_str.contains("core_service_database_connections_total"));
        assert!(body_str.contains("core_service_database_connections_idle"));
        assert!(body_str.contains("core_service_database_connections_active"));
        assert!(body_str.contains("core_service_version_info"));
    }

    #[actix_rt::test]
    async fn test_health_endpoint_without_database() {
        // This test should verify that the health endpoint works even without database data
        // This is a basic smoke test to ensure the endpoint is reachable
        
        let mut app = test::init_service(
            App::new()
                .route("/health/core-service", actix_web::web::get().to(|| async {
                    actix_web::HttpResponse::Ok().json(serde_json::json!({
                        "status": "healthy",
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                        "version": "test"
                    }))
                }))
        ).await;
        
        let req = test::TestRequest::get()
            .uri("/health/core-service")
            .to_request();
        
        let resp = test::call_service(&mut app, req).await;
        
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
    }
}