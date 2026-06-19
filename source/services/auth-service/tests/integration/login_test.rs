use actix_web::{http::StatusCode, test::TestRequest};
use sqlx::PgPool;

use crate::error::AuthError;

#[serial_test]
#[actix_web::test]
async fn test_successful_login_creates_user_profile(
    pool: PgPool,
) {
    // This test requires Keycloak to be running with test credentials
    // Test credentials: admin@bornemap.tn / test123

    // Prepare a mock login request
    let req = TestRequest::post()
        .uri("/api/v1/auth/login")
        .json(&serde_json::json!({
            "email": "admin@bornemap.tn",
            "password": "test123"
        }))
        .to_request();

    // TODO: Add actual test implementation
    // This would require:
    // 1. Keycloak to be running with the test user
    // 2. DB pool configured with auth_service_role
    // 3. Token verification logic

    // For now, we'll just check the endpoint returns 400 (no Keycloak)
    // let resp = app.to_owned().run(req).await.unwrap();
    // assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[serial_test]
#[actix_web::test]
async fn test_login_with_invalid_password_returns_401(
    pool: PgPool,
) {
    // This test would verify that an invalid password returns 401
    // without calling Keycloak

    let req = TestRequest::post()
        .uri("/api/v1/auth/login")
        .json(&serde_json::json!({
            "email": "admin@bornemap.tn",
            "password": "wrong_password"
        }))
        .to_request();

    // TODO: Add actual test implementation
    // The rate limiting middleware should also be tested
}

#[serial_test]
#[actix_web::test]
async fn test_login_with_empty_email_returns_400(
    pool: PgPool,
) {
    let req = TestRequest::post()
        .uri("/api/v1/auth/login")
        .json(&serde_json::json!({
            "email": "",
            "password": "test123"
        }))
        .to_request();

    // TODO: Add actual test implementation
}
