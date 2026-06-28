use std::sync::Once;

static INIT: Once = Once::new();

fn setup() {
    INIT.call_once(|| {
        dotenvy::dotenv().ok();
        let _ = env_logger::try_init();
        if std::env::var("JWT_SECRET").is_err() {
            std::env::set_var("JWT_SECRET", "test-secret-for-integration-tests");
        }
    });
}

fn get_database_url() -> Option<String> {
    std::env::var("DATABASE_URL").ok()
}

#[tokio::test]
async fn test_health_endpoint() {
    setup();

    let pool = match get_database_url() {
        Some(url) => sqlx::PgPool::connect(&url).await.unwrap(),
        None => {
            eprintln!("Skipping integration test: DATABASE_URL not set");
            return;
        }
    };

    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(actix_web::web::Data::new(pool))
            .configure(auth_service::config::routes::configure),
    )
    .await;

    let req = actix_web::test::TestRequest::get()
        .uri("/health")
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert!(resp.status().is_success());
}

#[tokio::test]
async fn test_register_and_login() {
    setup();

    let pool = match get_database_url() {
        Some(url) => sqlx::PgPool::connect(&url).await.unwrap(),
        None => {
            eprintln!("Skipping integration test: DATABASE_URL not set");
            return;
        }
    };

    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(actix_web::web::Data::new(pool.clone()))
            .configure(auth_service::config::routes::configure),
    )
    .await;

    // Register
    let register_body = serde_json::json!({
        "email": "test_integration@example.com",
        "password": "securePassword123",
        "role": "driver"
    });

    let req = actix_web::test::TestRequest::post()
        .uri("/auth/register")
        .set_json(&register_body)
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert!(
        resp.status().is_success() || resp.status() == actix_web::http::StatusCode::CONFLICT,
        "Register should succeed or return conflict if duplicate"
    );

    // Login
    let login_body = serde_json::json!({
        "email": "test_integration@example.com",
        "password": "securePassword123"
    });

    let req = actix_web::test::TestRequest::post()
        .uri("/auth/login")
        .set_json(&login_body)
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body: serde_json::Value = actix_web::test::read_body_json(resp).await;
    assert!(body.get("token").is_some());
    assert_eq!(body["email"], "test_integration@example.com");
    assert_eq!(body["role"], "driver");

    // Cleanup
    sqlx::query("DELETE FROM users.accounts WHERE email = 'test_integration@example.com'")
        .execute(&pool)
        .await
        .ok();
}
