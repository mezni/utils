use actix_web::{web, HttpResponse};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
    pub grant_type: String,
}

#[derive(Serialize)]
pub struct AuthResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in: i64,
    pub token_type: String,
}

pub async fn handle_login(
    req: web::Json<LoginRequest>,
) -> HttpResponse {
    // Get credentials from Keycloak
    let client_id = req.grant_type.as_str();

    if client_id != "password" {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "unsupported_grant_type",
            "error_description": "Only password grant type is supported"
        }));
    }

    // Call Keycloak token endpoint
    let keycloak_url = std::env::var("APP_KEYCLOAK_URL")
        .unwrap_or_else(|_| "http://localhost:8080".to_string());

    let token_url = format!("{}/realms/bornemap/protocol/openid-connect/token", keycloak_url);

    let response = reqwest::Client::new()
        .post(&token_url)
        .form(&req.into_inner())
        .send()
        .await;

    match response {
        Ok(resp) => {
            if resp.status().is_success() {
                let token_response: serde_json::Value = resp.json().await.unwrap_or_default();
                HttpResponse::Ok().json(token_response)
            } else {
                let error: serde_json::Value = resp.json().await.unwrap_or_default();
                HttpResponse::Unauthorized().json(error)
            }
        }
        Err(e) => {
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "keycloak_error",
                "error_description": e.to_string()
            }))
        }
    }
}

pub async fn handle_refresh_token(
    refresh_token: web::Json<serde_json::Value>,
) -> HttpResponse {
    let keycloak_url = std::env::var("APP_KEYCLOAK_URL")
        .unwrap_or_else(|_| "http://localhost:8080".to_string());

    let token_url = format!("{}/realms/bornemap/protocol/openid-connect/token", keycloak_url);

    let response = reqwest::Client::new()
        .post(&token_url)
        .form(refresh_token.into_inner())
        .send()
        .await;

    match response {
        Ok(resp) => {
            if resp.status().is_success() {
                let token_response: serde_json::Value = resp.json().await.unwrap_or_default();
                HttpResponse::Ok().json(token_response)
            } else {
                let error: serde_json::Value = resp.json().await.unwrap_or_default();
                HttpResponse::Unauthorized().json(error)
            }
        }
        Err(e) => {
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "keycloak_error",
                "error_description": e.to_string()
            }))
        }
    }
}
