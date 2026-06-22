use actix_web::body::BoxBody;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use auth_service::audit::emitter::AuditEmitter;
use auth_service::audit::middleware::audit_middleware;
use auth_service::config::AppConfig;

use auth_service::middleware::correlation::correlation_middleware;
use auth_service::middleware::jwt::{jwt_middleware, JwtConfig, JwtMiddleware};
use auth_service::sync::endpoint::handle_sync;

#[derive(Serialize, Deserialize)]
struct HealthResponse {
    status: String,
    timestamp: String,
    service: String,
}

async fn health() -> impl Responder {
    let response = HealthResponse {
        status: "ok".to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        service: "auth-service".to_string(),
    };
    HttpResponse::Ok().json(response)
}

async fn jwt_guard(
    req: ServiceRequest,
    next: Next<BoxBody>,
) -> Result<ServiceResponse<BoxBody>, actix_web::Error> {
    if req.path() == "/health" {
        return next.call(req).await;
    }

    let mw = req
        .app_data::<Arc<JwtMiddleware>>()
        .cloned()
        .ok_or_else(|| actix_web::error::ErrorInternalServerError("JWT middleware not configured"))?;

    let req = jwt_middleware(req, &mw).await?;
    next.call(req).await
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = AppConfig::from_env();
    let keycloak_url = config.keycloak_url.clone();

    let jwt_config = JwtConfig {
        jwks_uri: format!("{}/realms/bornemap/protocol/openid-connect/certs", keycloak_url),
        issuer: format!("{}/realms/bornemap", keycloak_url),
        audience: "auth-service-sa".to_string(),
        clock_skew_secs: 5,
    };

    let jwt_middleware = Arc::new(JwtMiddleware::new(jwt_config));
    let background_refresh = jwt_middleware.clone();

    let driver_service_url = std::env::var("APP_DRIVER_SERVICE_URL")
        .unwrap_or_else(|_| "http://localhost:3001".to_string());

    let audit_emitter = Arc::new(AuditEmitter::new(
        format!("{}/api/v1/telemetry/events", driver_service_url),
        config.auth_service_client_id.clone(),
    ));

    let port = config.server_port;

    tracing::info!("Starting auth-service on port {}", port);

    tokio::spawn(async move {
        if let Err(e) = background_refresh.refresh_cache().await {
            tracing::warn!("Initial JWKS cache refresh failed: {}", e);
        }
    });

    HttpServer::new(move || {
        let emitter = audit_emitter.clone();
        App::new()
            .app_data(jwt_middleware.clone())
            .app_data(audit_emitter.clone())
            .wrap(actix_web::middleware::from_fn(correlation_middleware))
            .wrap(actix_web::middleware::from_fn(jwt_guard))
            .wrap(actix_web::middleware::from_fn(move |req, next| {
                audit_middleware(req, next, emitter.clone())
            }))
            .route("/health", web::get().to(health))
            .route("/api/v1/auth/login", web::post().to(handle_login))
            .route("/api/v1/auth/refresh", web::post().to(handle_refresh_token))
            .route("/api/v1/auth/sync", web::get().to(handle_sync))
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await?;

    Ok(())
}
