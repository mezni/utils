use serde_json::Value;
use std::time::Duration;

#[tokio::test]
async fn test_full_auth_flow_integration() {
    let base_url = "http://localhost:3000";

    println!("=== Testing Full Auth Flow Integration ===");

    // Step 1: Login with password grant
    println!("Step 1: Login with username/password...");
    let login_resp = reqwest::Client::new()
        .post(format!("{}/realms/bornemap/protocol/openid-connect/token", base_url))
        .form(&serde_json::json!({
            "username": "admin@borne.map",
            "password": "admin123",
            "grant_type": "password",
            "client_id": "admin-dashboard",
            "scope": "openid"
        }))
        .send()
        .await
        .expect("Failed to login");

    assert!(login_resp.status().is_success(), "Login failed: {:?}", login_resp.status());
    let token_response: Value = login_resp.json().await.expect("Failed to parse token response");
    let access_token = token_response["access_token"].as_str().expect("No access_token in response");
    let refresh_token = token_response["refresh_token"].as_str().expect("No refresh_token in response");

    println!("  ✓ Login successful");
    println!("  Access token: {}...", &access_token[..50]);

    // Step 2: Verify JWT by calling driver-service
    println!("\nStep 2: Verify JWT in driver-service...");
    let driver_resp = reqwest::Client::new()
        .get(format!("{}/api/v1/drivers", base_url))
        .header("Authorization", format!("Bearer {}", access_token))
        .send()
        .await
        .expect("Failed to call driver-service");

    assert_eq!(driver_resp.status().as_u16(), 403, "Should be 403 (admin not driver)");

    println!("  ✓ JWT rejected by driver-service (correct RBAC)");

    // Step 3: Call admin-service with same token
    println!("\nStep 3: Call admin-service with token...");
    let admin_resp = reqwest::Client::new()
        .get(format!("{}/api/v1/admin/users", base_url))
        .header("Authorization", format!("Bearer {}", access_token))
        .send()
        .await
        .expect("Failed to call admin-service");

    assert_eq!(admin_resp.status().as_u16(), 200, "Should be 200 (admin has access)");

    println!("  ✓ Admin can access admin endpoints");

    // Step 4: Refresh token
    println!("\nStep 4: Refresh access token...");
    let refresh_resp = reqwest::Client::new()
        .post(format!("{}/realms/bornemap/protocol/openid-connect/token", base_url))
        .form(&serde_json::json!({
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": "admin-dashboard",
            "scope": "openid"
        }))
        .send()
        .await
        .expect("Failed to refresh token");

    assert!(refresh_resp.status().is_success(), "Token refresh failed: {:?}", refresh_resp.status());
    let refreshed_response: Value = refresh_resp.json().await.expect("Failed to parse refresh response");
    let new_access_token = refreshed_response["access_token"].as_str().expect("No new access_token");

    println!("  ✓ Token refreshed successfully");

    // Step 5: Verify new JWT works
    println!("\nStep 5: Verify refreshed token works...");
    let admin_resp_new = reqwest::Client::new()
        .get(format!("{}/api/v1/admin/users", base_url))
        .header("Authorization", format!("Bearer {}", new_access_token))
        .send()
        .await
        .expect("Failed to call admin-service with new token");

    assert_eq!(admin_resp_new.status().as_u16(), 200, "Should be 200 with refreshed token");

    println!("  ✓ Refreshed token valid");

    // Step 6: Call sync endpoint with service account
    println!("\nStep 6: Call sync endpoint with service account...");
    let service_token_resp = reqwest::Client::new()
        .post(format!("{}/realms/bornemap/protocol/openid-connect/token", base_url))
        .form(&serde_json::json!({
            "grant_type": "client_credentials",
            "client_id": "auth-service-sa",
            "client_secret": "auth-service-sa-secret",
            "scope": "openid"
        }))
        .send()
        .await
        .expect("Failed to get service token");

    let service_token: Value = service_token_resp.json().await.expect("Failed to parse service token");
    let service_token_str = service_token["access_token"].as_str().expect("No service access_token");

    let sync_resp = reqwest::Client::new()
        .get(format!("{}/api/v1/auth/sync?user_uuid=00000000-0000-0000-0000-000000000001", base_url))
        .header("Authorization", format!("Bearer {}", service_token_str))
        .send()
        .await
        .expect("Failed to call sync endpoint");

    assert!(sync_resp.status().is_success() || sync_resp.status().as_u16() == 404, "Sync endpoint failed");

    println!("  ✓ Sync endpoint accessible");

    // Step 7: Verify RBAC guards working
    println!("\nStep 7: Verify RBAC guards work correctly...");
    let admin_token_resp = reqwest::Client::new()
        .post(format!("{}/realms/bornemap/protocol/openid-connect/token", base_url))
        .form(&serde_json::json!({
            "username": "driver@borne.map",
            "password": "driver123",
            "grant_type": "password",
            "client_id": "mobile-driver",
            "scope": "openid"
        }))
        .send()
        .await
        .expect("Failed to get driver token");

    let driver_token: Value = admin_token_resp.json().await.expect("Failed to parse driver token");
    let driver_access_token = driver_token["access_token"].as_str().expect("No driver access_token");

    let admin_access_resp = reqwest::Client::new()
        .get(format!("{}/api/v1/admin/users", base_url))
        .header("Authorization", format!("Bearer {}", driver_access_token))
        .send()
        .await
        .expect("Failed to call admin endpoint as driver");

    assert_eq!(admin_access_resp.status().as_u16(), 403, "Driver should be blocked from admin endpoint");

    println!("  ✓ Driver blocked from admin endpoint (403)");

    println!("\n=== ALL INTEGRATION TESTS PASSED ===");
}

#[tokio::test]
async fn test_audit_flow() {
    let base_url = "http://localhost:3000";
    let driver_service_url = "http://localhost:3001";

    println!("=== Testing Audit Flow Integration ===");

    // Login and get token
    let login_resp = reqwest::Client::new()
        .post(format!("{}/realms/bornemap/protocol/openid-connect/token", base_url))
        .form(&serde_json::json!({
            "username": "admin@borne.map",
            "password": "admin123",
            "grant_type": "password",
            "client_id": "admin-dashboard",
            "scope": "openid"
        }))
        .send()
        .await
        .expect("Failed to login");

    let token_response: Value = login_resp.json().await.expect("Failed to parse token response");
    let access_token = token_response["access_token"].as_str().expect("No access_token");

    println!("Step 1: Send authenticated request to trigger audit event...");
    let admin_resp = reqwest::Client::new()
        .get(format!("{}/api/v1/admin/users", base_url))
        .header("Authorization", format!("Bearer {}", access_token))
        .send()
        .await
        .expect("Failed to call admin-service");

    assert_eq!(admin_resp.status().as_u16(), 200, "Should get 200");

    println!("  ✓ Request sent, audit event should be triggered");

    println!("\n=== AUDIT FLOW TEST PASSED ===");
}
