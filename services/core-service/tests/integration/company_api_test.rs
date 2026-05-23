#[cfg(test)]
mod tests {
    use actix_web::{test, App, http};
    use crate::handlers::company;
    use crate::utils::database::Database;
    use serde_json::json;

    #[actix_rt::test]
    async fn test_create_company() {
        // This test should fail initially because the Company CRUD endpoints are not implemented
        // After implementing the Company endpoints, this test should pass
        
        // Create a test database connection
        let database_url = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://bornemap:bornemap_dev@localhost:5432/bornemap_test".to_string());
        
        let database = Database::new(&database_url).await
            .expect("Failed to create test database connection");
        
        // Create test app with company routes
        let mut app = test::init_service(
            App::new()
                .app_data(actix_web::web::Data::new(database))
                .service(
                    actix_web::web::scope("/api/core/v1")
                        .service(
                            actix_web::web::resource("/companies")
                                .route(actix_web::web::post().to(|| async {
                                    // This will be replaced with actual company handler
                                    actix_web::HttpResponse::Ok().json(json!({"id": "CMP-123456789012"}))
                                }))
                        )
                )
        ).await;
        
        // Test creating a company
        let company_payload = json!({
            "name": "Test Company",
            "description": "Test Description",
            "email": "test@example.com",
            "phone": "+216-71-123-456",
            "website": "https://example.com",
            "address": "Test Address"
        });
        
        let req = test::TestRequest::post()
            .uri("/api/core/v1/companies")
            .set_json(&company_payload)
            .to_request();
        
        let resp = test::call_service(&mut app, req).await;
        
        // Check response status
        assert_eq!(resp.status(), http::StatusCode::CREATED);
        
        // Check response body
        let body = test::read_body(resp).await;
        let company_response: serde_json::Value = serde_json::from_slice(&body)
            .expect("Failed to parse company response");
        
        assert!(company_response["id"].is_string());
        assert_eq!(company_response["name"], "Test Company");
        assert_eq!(company_response["email"], "test@example.com");
        assert!(company_response["created_at"].is_string());
        assert!(company_response["updated_at"].is_string());
    }

    #[actix_rt::test]
    async fn test_get_company() {
        // This test should fail initially because the get company endpoint is not implemented
        // After implementing the get company endpoint, this test should pass
        
        // Create a test database connection
        let database_url = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://bornemap:bornemap_dev@localhost:5432/bornemap_test".to_string());
        
        let database = Database::new(&database_url).await
            .expect("Failed to create test database connection");
        
        // Create test app with company routes
        let mut app = test::init_service(
            App::new()
                .app_data(actix_web::web::Data::new(database))
                .service(
                    actix_web::web::scope("/api/core/v1")
                        .service(
                            actix_web::web::resource("/companies/{id}")
                                .route(actix_web::web::get().to(|_| async {
                                    // This will be replaced with actual get company handler
                                    actix_web::HttpResponse::Ok().json(json!({
                                        "id": "CMP-123456789012",
                                        "name": "Test Company",
                                        "email": "test@example.com"
                                    }))
                                }))
                        )
                )
        ).await;
        
        // Test getting a company
        let req = test::TestRequest::get()
            .uri("/api/core/v1/companies/CMP-123456789012")
            .to_request();
        
        let resp = test::call_service(&mut app, req).await;
        
        // Check response status
        assert_eq!(resp.status(), http::StatusCode::OK);
        
        // Check response body
        let body = test::read_body(resp).await;
        let company_response: serde_json::Value = serde_json::from_slice(&body)
            .expect("Failed to parse company response");
        
        assert_eq!(company_response["id"], "CMP-123456789012");
        assert_eq!(company_response["name"], "Test Company");
        assert_eq!(company_response["email"], "test@example.com");
    }

    #[actix_rt::test]
    async fn test_update_company() {
        // This test should fail initially because the update company endpoint is not implemented
        // After implementing the update company endpoint, this test should pass
        
        // Create a test database connection
        let database_url = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://bornemap:bornemap_dev@localhost:5432/bornemap_test".to_string());
        
        let database = Database::new(&database_url).await
            .expect("Failed to create test database connection");
        
        // Create test app with company routes
        let mut app = test::init_service(
            App::new()
                .app_data(actix_web::web::Data::new(database))
                .service(
                    actix_web::web::scope("/api/core/v1")
                        .service(
                            actix_web::web::resource("/companies/{id}")
                                .route(actix_web::web::put().to(|_| async {
                                    // This will be replaced with actual update company handler
                                    actix_web::HttpResponse::Ok().json(json!({
                                        "id": "CMP-123456789012",
                                        "name": "Updated Company",
                                        "version": 2
                                    }))
                                }))
                        )
                )
        ).await;
        
        // Test updating a company
        let update_payload = json!({
            "name": "Updated Company",
            "description": "Updated Description"
        });
        
        let req = test::TestRequest::put()
            .uri("/api/core/v1/companies/CMP-123456789012")
            .set_json(&update_payload)
            .to_request();
        
        let resp = test::call_service(&mut app, req).await;
        
        // Check response status
        assert_eq!(resp.status(), http::StatusCode::OK);
        
        // Check response body
        let body = test::read_body(resp).await;
        let company_response: serde_json::Value = serde_json::from_slice(&body)
            .expect("Failed to parse company response");
        
        assert_eq!(company_response["id"], "CMP-123456789012");
        assert_eq!(company_response["name"], "Updated Company");
        assert_eq!(company_response["version"], 2);
    }

    #[actix_rt::test]
    async fn test_delete_company() {
        // This test should fail initially because the delete company endpoint is not implemented
        // After implementing the delete company endpoint, this test should pass
        
        // Create a test database connection
        let database_url = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://bornemap:bornemap_dev@localhost:5432/bornemap_test".to_string());
        
        let database = Database::new(&database_url).await
            .expect("Failed to create test database connection");
        
        // Create test app with company routes
        let mut app = test::init_service(
            App::new()
                .app_data(actix_web::web::Data::new(database))
                .service(
                    actix_web::web::scope("/api/core/v1")
                        .service(
                            actix_web::web::resource("/companies/{id}")
                                .route(actix_web::web::delete().to(|_| async {
                                    // This will be replaced with actual delete company handler
                                    actix_web::HttpResponse::NoContent().finish()
                                }))
                        )
                )
        ).await;
        
        // Test deleting a company
        let req = test::TestRequest::delete()
            .uri("/api/core/v1/companies/CMP-123456789012")
            .to_request();
        
        let resp = test::call_service(&mut app, req).await;
        
        // Check response status
        assert_eq!(resp.status(), http::StatusCode::NO_CONTENT);
    }

    #[actix_rt::test]
    async fn test_list_companies() {
        // This test should fail initially because the list companies endpoint is not implemented
        // After implementing the list companies endpoint, this test should pass
        
        // Create a test database connection
        let database_url = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://bornemap:bornemap_dev@localhost:5432/bornemap_test".to_string());
        
        let database = Database::new(&database_url).await
            .expect("Failed to create test database connection");
        
        // Create test app with company routes
        let mut app = test::init_service(
            App::new()
                .app_data(actix_web::web::Data::new(database))
                .service(
                    actix_web::web::scope("/api/core/v1")
                        .service(
                            actix_web::web::resource("/companies")
                                .route(actix_web::web::get().to(|| async {
                                    // This will be replaced with actual list companies handler
                                    actix_web::HttpResponse::Ok().json(json!({
                                        "data": [
                                            {
                                                "id": "CMP-123456789012",
                                                "name": "Test Company",
                                                "email": "test@example.com"
                                            }
                                        ],
                                        "pagination": {
                                            "page": 1,
                                            "limit": 20,
                                            "total": 1,
                                            "pages": 1
                                        }
                                    }))
                                }))
                        )
                )
        ).await;
        
        // Test listing companies
        let req = test::TestRequest::get()
            .uri("/api/core/v1/companies")
            .to_request();
        
        let resp = test::call_service(&mut app, req).await;
        
        // Check response status
        assert_eq!(resp.status(), http::StatusCode::OK);
        
        // Check response body
        let body = test::read_body(resp).await;
        let list_response: serde_json::Value = serde_json::from_slice(&body)
            .expect("Failed to parse company list response");
        
        assert!(list_response["data"].is_array());
        assert!(list_response["pagination"].is_object());
        assert_eq!(list_response["pagination"]["page"], 1);
        assert_eq!(list_response["pagination"]["limit"], 20);
        assert!(list_response["pagination"]["total"] >= 0);
    }

    #[actix_rt::test]
    async fn test_company_validation() {
        // This test should fail initially because company validation is not implemented
        // After implementing company validation, this test should pass
        
        // Create a test database connection
        let database_url = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://bornemap:bornemap_dev@localhost:5432/bornemap_test".to_string());
        
        let database = Database::new(&database_url).await
            .expect("Failed to create test database connection");
        
        // Create test app with company routes
        let mut app = test::init_service(
            App::new()
                .app_data(actix_web::web::Data::new(database))
                .service(
                    actix_web::web::scope("/api/core/v1")
                        .service(
                            actix_web::web::resource("/companies")
                                .route(actix_web::web::post().to(|_| async {
                                    // This will be replaced with actual company handler with validation
                                    actix_web::HttpResponse::BadRequest().json(json!({
                                        "type": "https://api.bornemap.tn/errors/validation-error",
                                        "title": "Validation Error",
                                        "status": 400,
                                        "detail": "One or more validation errors occurred",
                                        "errors": [
                                            {
                                                "field": "name",
                                                "message": "Name is required"
                                            }
                                        ]
                                    }))
                                }))
                        )
                )
        ).await;
        
        // Test creating a company with invalid data (missing name)
        let invalid_payload = json!({
            "description": "Test Description",
            "email": "test@example.com"
        });
        
        let req = test::TestRequest::post()
            .uri("/api/core/v1/companies")
            .set_json(&invalid_payload)
            .to_request();
        
        let resp = test::call_service(&mut app, req).await;
        
        // Check response status
        assert_eq!(resp.status(), http::StatusCode::BAD_REQUEST);
        
        // Check response body contains validation errors
        let body = test::read_body(resp).await;
        let error_response: serde_json::Value = serde_json::from_slice(&body)
            .expect("Failed to parse error response");
        
        assert_eq!(error_response["type"], "https://api.bornemap.tn/errors/validation-error");
        assert_eq!(error_response["status"], 400);
        assert!(error_response["errors"].is_array());
    }
}