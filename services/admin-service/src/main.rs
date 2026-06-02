use axum::routing::get;
use axum::{Json, Router};
use common_auth::{auth_middleware, require_role, set_auth_config, AuthConfig};
use common_types::Role;
use serde_json::{json, Value};
use std::net::SocketAddr;
use tracing_subscriber::EnvFilter;

async fn health() -> Json<Value> {
    Json(json!({"status":"ok"}))
}

async fn admin_only() -> Json<Value> {
    Json(json!({
        "success": true,
        "data": {"message": "Admin access granted"},
        "meta": {}
    }))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let port: u16 = std::env::var("ADMIN_SERVICE_PORT")
        .unwrap_or_else(|_| "8082".into())
        .parse()
        .unwrap_or(8082);

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

    // Admin routes: authenticate first (innermost layer runs last), then enforce admin role.
    // Layers run bottom-to-top, so auth_middleware executes before require_role.
    let protected = Router::new()
        .route("/api/v1/admin/check", get(admin_only))
        .layer(axum::middleware::from_fn(require_role(Role::Admin)))
        .layer(axum::middleware::from_fn(auth_middleware));

    // /health is exempt from auth (liveness/readiness probes).
    let app = Router::new()
        .route("/health", get(health))
        .merge(protected);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("admin-service listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
