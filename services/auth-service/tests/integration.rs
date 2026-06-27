use actix_web::{App, test, web};
use auth_service::{config::AppConfig, http, infrastructure::jwt::JwtService};
use bornemap_db::{AppState, create_pool, run_migrations};
use std::sync::Arc;
use serde_json::json;

fn test_config() -> Option<AppConfig> {
    let database_url = std::env::var("TEST_DATABASE_URL").ok()?;
    Some(AppConfig {
        host: "127.0.0.1".into(),
        port: 0,
        database_url,
        jwt_secret: "test-integration-secret-key".into(),
        jwt_access_ttl_seconds: 3600,
        jwt_refresh_ttl_seconds: 86400,
        jwt_issuer: "test-issuer".into(),
        jwt_audience: "test-audience".into(),
        redis_url: "redis://localhost:6379".into(),
        rate_limit_requests: 100,
        rate_limit_window_seconds: 60,
        oauth_state_ttl_seconds: 300,
        google_client_id: None,
        google_client_secret: None,
        google_redirect_uri: None,
        google_auth_url: None,
        google_token_url: None,
        google_userinfo_url: None,
    })
}

fn create_oauth_state() -> http::oauth::OAuthState {
    use auth_service::application::oauth_state::OAuthStateStore;

    struct NoopStateStore;

    #[async_trait::async_trait]
    impl OAuthStateStore for NoopStateStore {
        async fn store_oauth_state(&self, _state: &str) -> Result<(), bornemap_core::AppError> {
            Ok(())
        }
        async fn validate_oauth_state(&self, _state: &str) -> Result<bool, bornemap_core::AppError> {
            Ok(true)
        }
        async fn create(&self, _state: &str, _ttl: std::time::Duration) -> Result<(), bornemap_core::AppError> {
            Ok(())
        }
        async fn consume(&self, _state: &str) -> Result<bool, bornemap_core::AppError> {
            Ok(true)
        }
    }

    http::oauth::OAuthState {
        google_provider: None,
        state_store: Arc::new(NoopStateStore),
    }
}

// Cannot share the init pattern due to impl Trait in return position,
// so each test initializes the app inline.

#[actix_web::test]
async fn health_live() {
    let config = match test_config() {
        Some(c) => c,
        None => {
            eprintln!("skipping: TEST_DATABASE_URL not set");
            return;
        }
    };
    let pool = create_pool(&config.database_url).await.unwrap();
    run_migrations(&pool).await.unwrap();
    let state = AppState { db: pool };
    let jwt_service = JwtService::new(
        config.jwt_secret,
        config.jwt_access_ttl_seconds,
        config.jwt_issuer,
        config.jwt_audience,
    );
    let oauth_state = create_oauth_state();
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .app_data(web::Data::new(jwt_service))
            .configure(|cfg| http::configure(cfg, oauth_state.clone())),
    )
    .await;

    let req = test::TestRequest::get().uri("/health/live").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
}

#[actix_web::test]
async fn register_success() {
    let config = match test_config() {
        Some(c) => c,
        None => {
            eprintln!("skipping: TEST_DATABASE_URL not set");
            return;
        }
    };
    let pool = create_pool(&config.database_url).await.unwrap();
    run_migrations(&pool).await.unwrap();
    let state = AppState { db: pool };
    let jwt_service = JwtService::new(
        config.jwt_secret,
        config.jwt_access_ttl_seconds,
        config.jwt_issuer,
        config.jwt_audience,
    );
    let oauth_state = create_oauth_state();
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .app_data(web::Data::new(jwt_service))
            .configure(|cfg| http::configure(cfg, oauth_state.clone())),
    )
    .await;

    let body = json!({"email": "integration-test@example.com", "password": "testpassword123"});
    let req = test::TestRequest::post()
        .uri("/api/v1/auth/register")
        .set_json(&body)
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 201);

    let json_body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(json_body["token_type"], "Bearer");
    assert!(!json_body["access_token"].as_str().unwrap().is_empty());
    assert_eq!(json_body["expires_in"], 86400);
}

#[actix_web::test]
async fn register_duplicate() {
    let config = match test_config() {
        Some(c) => c,
        None => {
            eprintln!("skipping: TEST_DATABASE_URL not set");
            return;
        }
    };
    let pool = create_pool(&config.database_url).await.unwrap();
    run_migrations(&pool).await.unwrap();
    let state = AppState { db: pool };
    let jwt_service = JwtService::new(
        config.jwt_secret,
        config.jwt_access_ttl_seconds,
        config.jwt_issuer,
        config.jwt_audience,
    );
    let oauth_state = create_oauth_state();
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .app_data(web::Data::new(jwt_service))
            .configure(|cfg| http::configure(cfg, oauth_state.clone())),
    )
    .await;

    let body = json!({"email": "dup-test@example.com", "password": "testpassword123"});
    let req = test::TestRequest::post()
        .uri("/api/v1/auth/register")
        .set_json(&body)
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 201);

    let req2 = test::TestRequest::post()
        .uri("/api/v1/auth/register")
        .set_json(&body)
        .to_request();
    let resp2 = test::call_service(&app, req2).await;
    assert_eq!(resp2.status(), 409);

    let json_body: serde_json::Value = test::read_body_json(resp2).await;
    assert_eq!(json_body["error"]["code"], "EMAIL_ALREADY_EXISTS");
}

#[actix_web::test]
async fn register_validation_error() {
    let config = match test_config() {
        Some(c) => c,
        None => {
            eprintln!("skipping: TEST_DATABASE_URL not set");
            return;
        }
    };
    let pool = create_pool(&config.database_url).await.unwrap();
    run_migrations(&pool).await.unwrap();
    let state = AppState { db: pool };
    let jwt_service = JwtService::new(
        config.jwt_secret,
        config.jwt_access_ttl_seconds,
        config.jwt_issuer,
        config.jwt_audience,
    );
    let oauth_state = create_oauth_state();
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .app_data(web::Data::new(jwt_service))
            .configure(|cfg| http::configure(cfg, oauth_state.clone())),
    )
    .await;

    let body = json!({"email": "invalid", "password": "short"});
    let req = test::TestRequest::post()
        .uri("/api/v1/auth/register")
        .set_json(&body)
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 400);
}

#[actix_web::test]
async fn login_success() {
    let config = match test_config() {
        Some(c) => c,
        None => {
            eprintln!("skipping: TEST_DATABASE_URL not set");
            return;
        }
    };
    let pool = create_pool(&config.database_url).await.unwrap();
    run_migrations(&pool).await.unwrap();
    let state = AppState { db: pool };
    let jwt_service = JwtService::new(
        config.jwt_secret,
        config.jwt_access_ttl_seconds,
        config.jwt_issuer,
        config.jwt_audience,
    );
    let oauth_state = create_oauth_state();
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .app_data(web::Data::new(jwt_service))
            .configure(|cfg| http::configure(cfg, oauth_state.clone())),
    )
    .await;

    let register_body = json!({"email": "login-test@example.com", "password": "testpassword123"});
    let req = test::TestRequest::post()
        .uri("/api/v1/auth/register")
        .set_json(&register_body)
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 201);

    let login_body = json!({"email": "login-test@example.com", "password": "testpassword123"});
    let req = test::TestRequest::post()
        .uri("/api/v1/auth/login")
        .set_json(&login_body)
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);

    let json_body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(json_body["token_type"], "Bearer");
    assert!(!json_body["access_token"].as_str().unwrap().is_empty());
}

#[actix_web::test]
async fn login_wrong_password() {
    let config = match test_config() {
        Some(c) => c,
        None => {
            eprintln!("skipping: TEST_DATABASE_URL not set");
            return;
        }
    };
    let pool = create_pool(&config.database_url).await.unwrap();
    run_migrations(&pool).await.unwrap();
    let state = AppState { db: pool };
    let jwt_service = JwtService::new(
        config.jwt_secret,
        config.jwt_access_ttl_seconds,
        config.jwt_issuer,
        config.jwt_audience,
    );
    let oauth_state = create_oauth_state();
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .app_data(web::Data::new(jwt_service))
            .configure(|cfg| http::configure(cfg, oauth_state.clone())),
    )
    .await;

    let register_body =
        json!({"email": "login-wrong-pw@example.com", "password": "correctpassword"});
    let req = test::TestRequest::post()
        .uri("/api/v1/auth/register")
        .set_json(&register_body)
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 201);

    let login_body = json!({"email": "login-wrong-pw@example.com", "password": "wrongpassword"});
    let req = test::TestRequest::post()
        .uri("/api/v1/auth/login")
        .set_json(&login_body)
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 409);

    let json_body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(json_body["error"]["code"], "INVALID_CREDENTIALS");
}

#[actix_web::test]
async fn login_nonexistent_user() {
    let config = match test_config() {
        Some(c) => c,
        None => {
            eprintln!("skipping: TEST_DATABASE_URL not set");
            return;
        }
    };
    let pool = create_pool(&config.database_url).await.unwrap();
    run_migrations(&pool).await.unwrap();
    let state = AppState { db: pool };
    let jwt_service = JwtService::new(
        config.jwt_secret,
        config.jwt_access_ttl_seconds,
        config.jwt_issuer,
        config.jwt_audience,
    );
    let oauth_state = create_oauth_state();
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .app_data(web::Data::new(jwt_service))
            .configure(|cfg| http::configure(cfg, oauth_state.clone())),
    )
    .await;

    let login_body = json!({"email": "nobody@example.com", "password": "somepassword"});
    let req = test::TestRequest::post()
        .uri("/api/v1/auth/login")
        .set_json(&login_body)
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 409);
}
