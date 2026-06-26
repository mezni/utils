#[cfg(test)]
mod tests {
    use actix_web::test;
    use actix_web::web::Data;
    use serde_json::json;
    use bornemap_db::AppState;
    use crate::infrastructure::jwt::JwtService;
    use crate::infrastructure::pg_session_repo::MockSessionRepository;
    use crate::infrastructure::pg_user_repo::MockUserRepository;
    use crate::application::register::RegisterUseCase;
    use crate::application::login::LoginUseCase;
    use crate::infrastructure::password::PasswordService;
    use bornemap_core::{User, UserRepository, AuthError, UserRole, UserStatus};
    use chrono::Utc;
    use uuid::Uuid;
    use std::collections::HashMap;
    use async_trait::async_trait;

    struct MockUserRepository {
        users: HashMap<String, User>,
        should_fail: bool,
    }

    impl MockUserRepository {
        fn new() -> Self {
            Self {
                users: HashMap::new(),
                should_fail: false,
            }
        }

        fn set_fail(&mut self, fail: bool) {
            self.should_fail = fail;
        }

        fn add_user(&mut self, email: &str, password_hash: &str) {
            let user = User {
                id: Uuid::new_v4(),
                email: email.to_string(),
                password_hash: password_hash.to_string(),
                role: UserRole::RegisteredDriver,
                status: UserStatus::Active,
                created_at: Utc::now(),
            };
            self.users.insert(email.to_lowercase(), user);
        }
    }

    #[async_trait]
    impl UserRepository for MockUserRepository {
        async fn create(&self, user: &User) -> Result<(), AuthError> {
            if self.should_fail {
                return Err(AuthError::InternalError);
            }
            Ok(())
        }

        async fn find_by_email(&self, email: &str) -> Result<Option<User>, AuthError> {
            Ok(self.users.get(&email.to_lowercase()).cloned())
        }

        async fn find_by_id(&self, id: UserId) -> Result<Option<User>, AuthError> {
            for user in self.users.values() {
                if user.id == id {
                    return Ok(Some(user.clone()));
                }
            }
            Ok(None)
        }

        async fn email_exists(&self, email: &str) -> Result<bool, AuthError> {
            Ok(self.users.contains_key(&email.to_lowercase()))
        }
    }

    async fn create_test_app() -> actix_web::test::TestApp {
        let user_repo = MockUserRepository::new();
        let session_repo = MockSessionRepository::new();
        let jwt_service = JwtService::new("test-secret".to_string(), 3600, "test-issuer", "test-audience");
        
        let app_data = AppState {
            db: sqlx::PgPool::connect("postgresql://localhost:5432/test").await.unwrap(),
        };
        
        // Create test app with our routes
        test::init_service(
            actix_web::App::new()
                .app_data(Data::new(app_data))
                .app_data(Data::new(jwt_service))
                .service(crate::http::auth::register)
                .service(crate::http::auth::login)
                .service(crate::http::auth::refresh)
        ).await
    }

    #[actix_web::test]
    async fn test_register_success() {
        let mut app = create_test_app().await;
        
        let req_body = json!({
            "email": "test@example.com",
            "password": "ValidPassword123!"
        });

        let req = test::TestRequest::post()
            .uri("/api/v1/auth/register")
            .set_json(&req_body)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::CREATED);

        let body = test::read_body(&resp).await;
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        
        // Check response structure
        assert!(response["data"].is_object());
        assert!(response["data"]["access_token"].is_string());
        assert!(response["data"]["token_type"].is_string());
        assert_eq!(response["data"]["token_type"], "Bearer");
        assert_eq!(response["data"]["expires_in"], 86400);
        
        // Check metadata
        assert!(response["meta"]["request_id"].is_string());
        assert!(response["meta"]["timestamp"].is_string());
        
        // Check error is null
        assert!(response["error"].is_null());
    }

    #[actix_web::test]
    async fn test_register_duplicate_email() {
        let mut app = create_test_app().await;
        
        // Register a user first
        let req_body = json!({
            "email": "test@example.com",
            "password": "ValidPassword123!"
        });

        let req = test::TestRequest::post()
            .uri("/api/v1/auth/register")
            .set_json(&req_body)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::CREATED);

        // Try to register the same user again
        let req = test::TestRequest::post()
            .uri("/api/v1/auth/register")
            .set_json(&req_body)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::CONFLICT);

        let body = test::read_body(&resp).await;
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        
        // Check error structure
        assert!(response["data"].is_null());
        assert!(response["error"].is_object());
        assert_eq!(response["error"]["code"], "USER_ALREADY_EXISTS");
        assert!(response["error"]["message"].is_string());
        assert!(response["meta"]["request_id"].is_string());
    }

    #[actix_web::test]
    async fn test_register_invalid_email() {
        let mut app = create_test_app().await;
        
        let req_body = json!({
            "email": "invalid-email",
            "password": "ValidPassword123!"
        });

        let req = test::TestRequest::post()
            .uri("/api/v1/auth/register")
            .set_json(&req_body)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);

        let body = test::read_body(&resp).await;
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        
        // Check error structure
        assert!(response["data"].is_null());
        assert!(response["error"].is_object());
        assert_eq!(response["error"]["code"], "VALIDATION_ERROR");
        assert!(response["error"]["message"].is_string());
        assert!(response["error"]["details"].is_string());
    }

    #[actix_web::test]
    async fn test_register_invalid_password() {
        let mut app = create_test_app().await;
        
        let req_body = json!({
            "email": "test@example.com",
            "password": "short"
        });

        let req = test::TestRequest::post()
            .uri("/api/v1/auth/register")
            .set_json(&req_body)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);

        let body = test::read_body(&resp).await;
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        
        // Check error structure
        assert!(response["data"].is_null());
        assert!(response["error"].is_object());
        assert_eq!(response["error"]["code"], "VALIDATION_ERROR");
        assert!(response["error"]["message"].is_string());
        assert!(response["error"]["details"].is_string());
    }

    #[actix_web::test]
    async fn test_register_missing_fields() {
        let mut app = create_test_app().await;
        
        let req_body = json!({
            "email": "",
            "password": ""
        });

        let req = test::TestRequest::post()
            .uri("/api/v1/auth/register")
            .set_json(&req_body)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);

        let body = test::read_body(&resp).await;
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        
        // Check error structure
        assert!(response["data"].is_null());
        assert!(response["error"].is_object());
        assert_eq!(response["error"]["code"], "VALIDATION_ERROR");
        assert!(response["error"]["message"].is_string());
    }

    #[actix_web::test]
    async fn test_login_success() {
        let mut app = create_test_app().await;
        
        // Register a user first
        let req_body = json!({
            "email": "test@example.com",
            "password": "ValidPassword123!"
        });

        let req = test::TestRequest::post()
            .uri("/api/v1/auth/register")
            .set_json(&req_body)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::CREATED);

        // Login with the same user
        let req_body = json!({
            "email": "test@example.com",
            "password": "ValidPassword123!"
        });

        let req = test::TestRequest::post()
            .uri("/api/v1/auth/login")
            .set_json(&req_body)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);

        let body = test::read_body(&resp).await;
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        
        // Check response structure
        assert!(response["data"].is_object());
        assert!(response["data"]["access_token"].is_string());
        assert!(response["data"]["refresh_token"].is_string());
        assert_eq!(response["data"]["token_type"], "Bearer");
        assert_eq!(response["data"]["expires_in"], 86400);
        
        // Check metadata
        assert!(response["meta"]["request_id"].is_string());
        
        // Check error is null
        assert!(response["error"].is_null());
    }

    #[actix_web::test]
    async fn test_login_wrong_password() {
        let mut app = create_test_app().await;
        
        // Register a user first
        let req_body = json!({
            "email": "test@example.com",
            "password": "ValidPassword123!"
        });

        let req = test::TestRequest::post()
            .uri("/api/v1/auth/register")
            .set_json(&req_body)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::CREATED);

        // Login with wrong password
        let req_body = json!({
            "email": "test@example.com",
            "password": "wrong_password"
        });

        let req = test::TestRequest::post()
            .uri("/api/v1/auth/login")
            .set_json(&req_body)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::UNAUTHORIZED);

        let body = test::read_body(&resp).await;
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        
        // Check error structure
        assert!(response["data"].is_null());
        assert!(response["error"].is_object());
        assert_eq!(response["error"]["code"], "INVALID_CREDENTIALS");
        assert!(response["error"]["message"].is_string());
    }

    #[actix_web::test]
    async fn test_login_unknown_user() {
        let mut app = create_test_app().await;
        
        let req_body = json!({
            "email": "unknown@example.com",
            "password": "any_password"
        });

        let req = test::TestRequest::post()
            .uri("/api/v1/auth/login")
            .set_json(&req_body)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::UNAUTHORIZED);

        let body = test::read_body(&resp).await;
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        
        // Check error structure
        assert!(response["data"].is_null());
        assert!(response["error"].is_object());
        assert_eq!(response["error"]["code"], "INVALID_CREDENTIALS");
        assert!(response["error"]["message"].is_string());
    }

    #[actix_web::test]
    async fn test_login_missing_fields() {
        let mut app = create_test_app().await;
        
        let req_body = json!({
            "email": "",
            "password": ""
        });

        let req = test::TestRequest::post()
            .uri("/api/v1/auth/login")
            .set_json(&req_body)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);

        let body = test::read_body(&resp).await;
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        
        // Check error structure
        assert!(response["data"].is_null());
        assert!(response["error"].is_object());
        assert_eq!(response["error"]["code"], "VALIDATION_ERROR");
        assert!(response["error"]["message"].is_string());
    }

    #[actix_web::test]
    async fn test_refresh_token_success() {
        let mut app = create_test_app().await;
        
        // Register and login a user first
        let req_body = json!({
            "email": "test@example.com",
            "password": "ValidPassword123!"
        });

        let req = test::TestRequest::post()
            .uri("/api/v1/auth/register")
            .set_json(&req_body)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::CREATED);

        let req = test::TestRequest::post()
            .uri("/api/v1/auth/login")
            .set_json(&req_body)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);

        let body = test::read_body(&resp).await;
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let refresh_token = response["data"]["refresh_token"].as_str().unwrap();

        // Refresh the token
        let req_body = json!({
            "refresh_token": refresh_token
        });

        let req = test::TestRequest::post()
            .uri("/api/v1/auth/refresh")
            .set_json(&req_body)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);

        let body = test::read_body(&resp).await;
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        
        // Check response structure
        assert!(response["data"].is_object());
        assert!(response["data"]["access_token"].is_string());
        assert!(response["data"]["refresh_token"].is_string());
        assert_eq!(response["data"]["token_type"], "Bearer");
        assert_eq!(response["data"]["expires_in"], 86400);
    }

    #[actix_web::test]
    async fn test_refresh_invalid_token() {
        let mut app = create_test_app().await;
        
        let req_body = json!({
            "refresh_token": "invalid_token"
        });

        let req = test::TestRequest::post()
            .uri("/api/v1/auth/refresh")
            .set_json(&req_body)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::UNAUTHORIZED);

        let body = test::read_body(&resp).await;
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        
        // Check error structure
        assert!(response["data"].is_null());
        assert!(response["error"].is_object());
        assert_eq!(response["error"]["code"], "INVALID_CREDENTIALS");
        assert!(response["error"]["message"].is_string());
    }

    #[actix_web::test]
    async fn test_malformed_json() {
        let mut app = create_test_app().await;
        
        let req_body = "invalid json {";

        let req = test::TestRequest::post()
            .uri("/api/v1/auth/register")
            .set_body(req_body)
            .to_request();

        let resp = test::call_service(&app, req).await;
        // Actix should handle malformed JSON automatically
        assert!(resp.status().is_client_error());
    }

    #[actix_web::test]
    async fn test_request_id_header() {
        let mut app = create_test_app().await;
        
        let req_body = json!({
            "email": "test@example.com",
            "password": "ValidPassword123!"
        });

        let req = test::TestRequest::post()
            .uri("/api/v1/auth/register")
            .set_json(&req_body)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::CREATED);

        // Check if X-Request-ID header is present
        let headers = resp.headers();
        assert!(headers.contains_key("x-request-id"));
        
        let request_id = headers.get("x-request-id").unwrap();
        assert!(!request_id.is_empty());
    }
}