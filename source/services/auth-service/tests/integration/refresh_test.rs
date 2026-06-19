use actix_web::{http::StatusCode, test::TestRequest};

#[serial_test]
#[actix_web::test]
async fn test_successful_refresh_updates_last_login(
    pool: PgPool,
) {
    // This test would verify that a successful refresh updates the last_login_at timestamp

    // TODO: Add actual test implementation
    // Test steps:
    // 1. Login to get tokens
    // 2. Store refresh_token
    // 3. Call /refresh with refresh_token
    // 4. Verify last_login_at was updated
}

#[serial_test]
#[actix_web::test]
async fn test_refresh_with_expired_token_returns_401(
    pool: PgPool,
) {
    // This test would verify that an expired refresh token returns 401

    let req = TestRequest::post()
        .uri("/api/v1/auth/refresh")
        .json(&serde_json::json!({
            "refresh_token": "expired_token"
        }))
        .to_request();

    // TODO: Add actual test implementation
}

#[serial_test]
#[actix_web::test]
async fn test_refresh_with_invalid_format_returns_400(
    pool: PgPool,
) {
    // This test would verify that a malformed refresh token returns 400
    // WITHOUT contacting Keycloak

    let req = TestRequest::post()
        .uri("/api/v1/auth/refresh")
        .json(&serde_json::json!({
            "refresh_token": "invalid_format"
        }))
        .to_request();

    // TODO: Add actual test implementation
    // Should return 400 without calling Keycloak
}
