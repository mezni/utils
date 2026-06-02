use axum::{routing::get, Extension, Json, Router};
use common_auth::{auth_middleware, set_auth_config, AuthConfig, CurrentUser};
use serde_json::{json, Value};
use std::net::SocketAddr;
use tracing_subscriber::EnvFilter;

async fn health() -> Json<Value> {
    Json(json!({"status":"ok"}))
}

async fn me(Extension(user): Extension<CurrentUser>) -> Json<Value> {
    Json(json!({
        "success": true,
        "data": {
            "user_id": user.user_id,
            "keycloak_user_id": user.keycloak_user_id,
            "email": user.email,
            "role": user.role,
            "partner_id": user.partner_id,
        },
        "meta": {}
    }))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let port: u16 = std::env::var("DRIVER_SERVICE_PORT")
        .unwrap_or_else(|_| "8081".into())
        .parse()
        .unwrap_or(8081);

    let issuer = std::env::var("AUTH_ISSUER")
        .unwrap_or_else(|_| "http://keycloak:8080/realms/bornemap".into());
    let jwks_url = std::env::var("AUTH_JWKS_URL")
        .unwrap_or_else(|_| "http://keycloak:8080/realms/bornemap/protocol/openid-connect/certs".into());
    let audience = std::env::var("AUTH_AUDIENCE")
        .unwrap_or_else(|_| "bornemap-api".into());

    set_auth_config(AuthConfig {
        issuer: issuer.clone(),
        audience: audience.clone(),
        jwks_url: jwks_url.clone(),
    });

    common_auth::init_jwks_cache(jwks_url).await;

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/driver/me", get(me))
        .layer(axum::middleware::from_fn(auth_middleware));

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("driver-service listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
