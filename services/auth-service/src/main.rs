use actix_web::web;
use actix_web::{App, HttpServer};
use auth_service::http::middleware::logging::LoggingMiddleware;
use auth_service::http::middleware::rate_limit::{RateLimitConfig, RateLimitMiddlewareFactory};
use auth_service::http::middleware::request_id::RequestIdMiddleware;
use auth_service::http::middleware::tracing::TracingMiddleware;
use auth_service::http::metrics::{MetricsMiddlewareFactory, PrometheusMetrics};
use auth_service::{config, http, infrastructure};
use bornemap_db::{AppState, create_pool, run_migrations, RedisClient};
use config::AppConfig;
use http::oauth::OAuthState;
use infrastructure::oauth::google::GoogleOAuthProvider;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();

    let config = AppConfig::from_env().expect("configuration failed");

    auth_service::redis_config::RedisConfig::from_env()
        .expect("Failed to load Redis configuration")
        .validate()
        .expect("Redis configuration validation failed");

    let pool = create_pool(&config.database_url)
        .await
        .expect("DB connection failed");

    sqlx::query("SELECT 1")
        .execute(&pool)
        .await
        .expect("DB not reachable");

    run_migrations(&pool).await.expect("Migration failed");

    let redis_client = Arc::new(
        RedisClient::new(&config.redis_url)
            .expect("Failed to create Redis client"),
    );

    let oauth_state_store = Arc::new(
        auth_service::application::oauth_state::RedisOAuthStateStore::new(
            redis_client.clone(),
        ),
    );

    let google_provider = config.google_client_id.clone().map(|client_id| {
        GoogleOAuthProvider::new(
            client_id,
            config.google_client_secret.clone().unwrap_or_default(),
            config
                .google_redirect_uri
                .clone()
                .unwrap_or_else(|| "http://localhost:8080/api/v1/auth/oauth/google/callback".to_string()),
        )
    });

    let oauth_state = OAuthState {
        google_provider,
        state_store: oauth_state_store,
    };

    let rate_limit_config = RateLimitConfig {
        ip_limit: config.rate_limit_requests as u64,
        user_limit: config.rate_limit_requests as u64,
        window_seconds: config.rate_limit_window_seconds,
        sensitive_endpoint_multiplier: 4,
    };
    let rate_limiter = RateLimitMiddlewareFactory::new(
        rate_limit_config,
        redis_client,
    );

    let state = AppState { db: pool };
    let jwt_service = infrastructure::jwt::JwtService::new(
        config.jwt_secret.clone(),
        config.jwt_access_ttl_seconds,
        config.jwt_issuer.clone(),
        config.jwt_audience.clone(),
    );

    let metrics = Arc::new(
        PrometheusMetrics::new().expect("Failed to initialize Prometheus metrics"),
    );
    let metrics_mw = MetricsMiddlewareFactory::new(metrics.clone());

    println!("auth-service running on {}:{}", config.host, config.port);

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(state.clone()))
            .app_data(web::Data::new(jwt_service.clone()))
            .app_data(web::Data::from(metrics.clone()))
            .app_data(web::Data::new(oauth_state.clone()))
            .wrap(RequestIdMiddleware)
            .wrap(TracingMiddleware)
            .wrap(metrics_mw.clone())
            .wrap(rate_limiter.clone())
            .wrap(LoggingMiddleware)
            .configure(|cfg| http::configure(cfg, oauth_state.clone()))
    })
    .bind((config.host, config.port))?
    .run()
    .await
}
