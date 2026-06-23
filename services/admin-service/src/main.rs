use actix_web::body::BoxBody;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use actix_web::{web, App, Error, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use sqlx::postgres::PgPool;

use admin_service::api::configure_routes;
use admin_service::identity::sync::{identity_sync_middleware, SyncMiddleware};
use admin_service::middleware::correlation::correlation_middleware;
use admin_service::middleware::jwt::{jwt_middleware, JwtConfig, JwtMiddleware};
use admin_service::services::{CacheService, KPIAggregationEngine, KPIConfig};

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
        service: "admin-service".to_string(),
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

/// Shared application state
pub struct AppState {
    pub kpi_engine: Arc<KPIAggregationEngine>,
    pub db_pool: Arc<sqlx::postgres::PgPool>,
    pub cache_service: Arc<CacheService>,
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let keycloak_url = std::env::var("APP_KEYCLOAK_URL")
        .unwrap_or_else(|_| "http://localhost:8080".to_string());

    let jwt_config = JwtConfig {
        jwks_uri: format!("{}/realms/bornemap/protocol/openid-connect/certs", keycloak_url),
        issuer: format!("{}/realms/bornemap", keycloak_url),
        audience: "admin-service-sa".to_string(),
        clock_skew_secs: 5,
    };

    let jwt_middleware = Arc::new(JwtMiddleware::new(jwt_config));
    let background_refresh = jwt_middleware.clone();

    let port: u16 = std::env::var("APP_SERVER_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3002);

    tracing::info!("Starting admin-service on port {}", port);

    // Initialize database connection
    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:pass@localhost:5432/platform_db".to_string());

    let db_pool = Arc::new(
        sqlx::postgres::PgPool::connect(&db_url)
            .await
            .context("Failed to connect to database")?
    );

    // Initialize cache service
    let cache_config = crate::services::CacheConfig {
        redis_url: std::env::var("REDIS_URL")
            .unwrap_or_else(|_| "redis://localhost:6379".to_string()),
        default_ttl_seconds: 300,
        max_connections: 10,
    };

    let cache_service = Arc::new(
        CacheService::new(cache_config).await
            .context("Failed to initialize cache service")?
    );

    // Initialize KPI aggregation engine
    let kpi_config = KPIConfig::default();
    let kpi_engine = Arc::new(KPIAggregationEngine::new(
        kpi_config,
        db_pool.clone(),
        cache_service.clone(),
    ));

    // Create app state
    let app_state = Arc::new(AppState {
        kpi_engine,
        db_pool,
        cache_service,
    });

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
            .configure(|cfg| configure_routes(cfg, app_state.clone()))
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await?;

    Ok(())
}
