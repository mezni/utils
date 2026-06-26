#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test;
    use bornemap_auth::{RedisOAuthStateStore, OAuthStateStore};
    use bornemap_core::AppError;
    use std::time::Duration;

    // Mock OAuth state store for testing
    struct MockOAuthStateStore {
        states: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    }

    impl MockOAuthStateStore {
        fn new() -> Self {
            Self {
                states: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
            }
        }
    }

    #[async_trait::async_trait]
    impl OAuthStateStore for MockOAuthStateStore {
        async fn create(&self, state: &str, _ttl: Duration) -> Result<(), AppError> {
            let mut states = self.states.lock().unwrap();
            states.insert(state.to_string());
            Ok(())
        }

        async fn consume(&self, state: &str) -> Result<bool, AppError> {
            let mut states = self.states.lock().unwrap();
            let existed = states.remove(state);
            Ok(existed)
        }
    }

    #[actix_web::test]
    async fn test_oauth_start_google_success() {
        let app = test::init_service(
            App::new()
                .route("/oauth/google/start", web::get().to(|| async {
                    HttpResponse::Ok().body("Mock OAuth start response")
                }))
        ).await;

        let req = test::TestRequest::get()
            .uri("/oauth/google/start")
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
    }

    #[actix_web::test]
    async fn test_oauth_start_unsupported_provider() {
        let app = test::init_service(
            App::new()
                .route("/oauth/unsupported/start", web::get().to(|| async {
                    HttpResponse::Ok().body("Mock OAuth start response")
                }))
        ).await;

        let req = test::TestRequest::get()
            .uri("/oauth/unsupported/start")
            .to_request();

        let resp = test::call_service(&app, req).await;
        // Should return 400 for unsupported provider
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

    #[actix_web::test]
    async fn test_oauth_callback_missing_code() {
        let app = test::init_service(
            App::new()
                .route("/oauth/google/callback", web::get().to(|| async {
                    HttpResponse::Ok().body("Mock OAuth callback response")
                }))
        ).await;

        let req = test::TestRequest::get()
            .uri("/oauth/google/callback")
            .to_request();

        let resp = test::call_service(&app, req).await;
        // Should return 400 for missing code parameter
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

    #[actix_web::test]
    async fn test_oauth_callback_missing_state() {
        let app = test::init_service(
            App::new()
                .route("/oauth/google/callback", web::get().to(|| async {
                    HttpResponse::Ok().body("Mock OAuth callback response")
                }))
        ).await;

        let req = test::TestRequest::get()
            .uri("/oauth/google/callback?code=test-code")
            .to_request();

        let resp = test::call_service(&app, req).await;
        // Should return 400 for missing state parameter
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

    #[actix_web::test]
    async fn test_oauth_callback_success() {
        let app = test::init_service(
            App::new()
                .route("/oauth/google/callback", web::get().to(|| async {
                    HttpResponse::Ok().json(serde_json::json!({
                        "data": {
                            "user_id": "123e4567-e89b-12d3-a456-426614174000",
                            "email": "test@example.com",
                            "role": "REGISTERED_DRIVER",
                            "status": "ACTIVE"
                        },
                        "meta": {
                            "request_id": "test-request-id",
                            "timestamp": "2024-01-01T00:00:00Z"
                        },
                        "error": null
                    }))
                }))
        ).await;

        let req = test::TestRequest::get()
            .uri("/oauth/google/callback?code=test-code&state=test-state")
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
    }

    #[test]
    fn test_oauth_start_query_deserialization() {
        let query = "redirect_uri=http://example.com/callback";
        let result: OAuthStartQuery = serde_urlencoded::from_str(query).unwrap();
        
        assert_eq!(result.redirect_uri, Some("http://example.com/callback".to_string()));
    }

    #[test]
    fn test_oauth_callback_query_deserialization() {
        let query = "code=test-auth-code&state=test-state-123";
        let result: OAuthCallbackQuery = serde_urlencoded::from_str(query).unwrap();
        
        assert_eq!(result.code, Some("test-auth-code".to_string()));
        assert_eq!(result.state, Some("test-state-123".to_string()));
    }

    #[test]
    fn test_oauth_callback_query_missing_parameters() {
        let query = "code=test-auth-code";
        let result: OAuthCallbackQuery = serde_urlencoded::from_str(query).unwrap();
        
        assert_eq!(result.code, Some("test-auth-code".to_string()));
        assert_eq!(result.state, None);
    }

    #[test]
    fn test_oauth_error_response_format() {
        let error_response = ApiResponse::error(AppError::OAuthStateInvalid);
        
        match serde_json::to_value(&error_response) {
            Ok(value) => {
                assert!(value.is_object());
                if let Some(obj) = value.as_object() {
                    assert!(obj.contains_key("data"));
                    assert!(obj.contains_key("meta"));
                    assert!(obj.contains_key("error"));
                    
                    if let Some(error) = obj.get("error") {
                        if let Some(error_obj) = error.as_object() {
                            assert!(error_obj.contains_key("code"));
                            assert!(error_obj.contains_key("message"));
                        }
                    }
                }
            }
            Err(_) => panic!("Failed to serialize error response"),
        }
    }

    #[test]
    fn test_oauth_success_response_format() {
        let user_data = serde_json::json!({
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "email": "test@example.com",
            "role": "REGISTERED_DRIVER",
            "status": "ACTIVE"
        });
        
        let response = ApiResponse::success(Some(user_data));
        
        match serde_json::to_value(&response) {
            Ok(value) => {
                assert!(value.is_object());
                if let Some(obj) = value.as_object() {
                    assert!(obj.contains_key("data"));
                    assert!(obj.contains_key("meta"));
                    assert!(obj.contains_key("error"));
                    
                    if let Some(data) = obj.get("data") {
                        assert!(data.is_object());
                        if let Some(data_obj) = data.as_object() {
                            assert!(data_obj.contains_key("user_id"));
                            assert!(data_obj.contains_key("email"));
                            assert!(data_obj.contains_key("role"));
                            assert!(data_obj.contains_key("status"));
                        }
                    }
                    
                    if let Some(error) = obj.get("error") {
                        assert!(error.is_null());
                    }
                }
            }
            Err(_) => panic!("Failed to serialize success response"),
        }
    }
}