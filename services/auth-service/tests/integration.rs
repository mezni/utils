use actix_web::{App, test, web};
use auth_service::{config::AppConfig, http, infrastructure::jwt::JwtService};
use bornemap_db::{create_pool, run_migrations, AppState};
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
    })
}

// Cannot share the init pattern due to impl Trait in return position,
// so each test initializes the app inline.

#[actix_web::test]
async fn health_live() {
    let config = match test_config() {
        Some(c) => c,
        None => { eprintln!("skipping: TEST_DATABASE_URL not set"); return; }
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
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .app_data(web::Data::new(jwt_service))
            .configure(http::configure),
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
        None => { eprintln!("skipping: TEST_DATABASE_URL not set"); return; }
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
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .app_data(web::Data::new(jwt_service))
            .configure(http::configure),
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
    assert!(json_body["access_token"].as_str().unwrap().len() > 0);
    assert_eq!(json_body["expires_in"], 86400);
}

#[actix_web::test]
async fn register_duplicate() {
    let config = match test_config() {
        Some(c) => c,
        None => { eprintln!("skipping: TEST_DATABASE_URL not set"); return; }
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
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .app_data(web::Data::new(jwt_service))
            .configure(http::configure),
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
        None => { eprintln!("skipping: TEST_DATABASE_URL not set"); return; }
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
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .app_data(web::Data::new(jwt_service))
            .configure(http::configure),
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
        None => { eprintln!("skipping: TEST_DATABASE_URL not set"); return; }
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
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .app_data(web::Data::new(jwt_service))
            .configure(http::configure),
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
    assert!(json_body["access_token"].as_str().unwrap().len() > 0);
}

#[actix_web::test]
async fn login_wrong_password() {
    let config = match test_config() {
        Some(c) => c,
        None => { eprintln!("skipping: TEST_DATABASE_URL not set"); return; }
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
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .app_data(web::Data::new(jwt_service))
            .configure(http::configure),
    )
    .await;

    let register_body = json!({"email": "login-wrong-pw@example.com", "password": "correctpassword"});
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
        None => { eprintln!("skipping: TEST_DATABASE_URL not set"); return; }
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
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .app_data(web::Data::new(jwt_service))
            .configure(http::configure),
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
