use actix_web::body::BoxBody;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use driver_service::api::telemetry::{ingest_events, configure_routes};
use driver_service::identity::sync::{identity_sync_middleware, SyncMiddleware};
use driver_service::middleware::correlation::correlation_middleware;
use driver_service::middleware::jwt::{jwt_middleware, JwtConfig, JwtMiddleware};

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
        service: "driver-service".to_string(),
    };
    HttpResponse::Ok().json(response)
}

async fn jwt_guard(
    req: ServiceRequest,
    next: Next<BoxBody>,
) -> Result<ServiceResponse<BoxBody>, actix_web::Error> {
    if req.path() == "/health" || req.path() == "/api/v1/telemetry/events" {
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

    let keycloak_url = std::env::var("APP_KEYCLOAK_URL")
        .unwrap_or_else(|_| "http://localhost:8080".to_string());

    let jwt_config = JwtConfig {
        jwks_uri: format!("{}/realms/bornemap/protocol/openid-connect/certs", keycloak_url),
        issuer: format!("{}/realms/bornemap", keycloak_url),
        audience: "driver-service-sa".to_string(),
        clock_skew_secs: 5,
    };

    let jwt_middleware = Arc::new(JwtMiddleware::new(jwt_config));
    let background_refresh = jwt_middleware.clone();

    let port: u16 = std::env::var("APP_SERVER_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3001);

    tracing::info!("Starting driver-service on port {}", port);

    tokio::spawn(async move {
        if let Err(e) = background_refresh.refresh_cache().await {
            tracing::warn!("Initial JWKS cache refresh failed: {}", e);
        }
    });

    HttpServer::new(move || {
        App::new()
            .app_data(jwt_middleware.clone())
            .wrap(actix_web::middleware::from_fn(correlation_middleware))
            .wrap(actix_web::middleware::from_fn(jwt_guard))
            .wrap(actix_web::middleware::from_fn(identity_sync_middleware))
            .route("/health", web::get().to(health))
            .configure(configure_routes)
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await?;

    Ok(())
}
