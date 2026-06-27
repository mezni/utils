use actix_web::web;
use actix_web::{App, HttpServer};
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

    // Validate Redis configuration
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

    // Initialize Redis client
    let redis_client = Arc::new(
        RedisClient::new(&config.redis_url)
            .expect("Failed to create Redis client"),
    );

    // Initialize OAuth components
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

    // Initialize rate limiting middleware
    let rate_limit_config = auth_service::http::middleware::RateLimitConfig {
        max_requests: config.rate_limit_requests as u64,
        window_seconds: config.rate_limit_window_seconds,
    };
    let rate_limiter = auth_service::http::middleware::RateLimitMiddlewareFactory::new(
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

    println!("auth-service running on {}:{}", config.host, config.port);

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(state.clone()))
            .app_data(web::Data::new(jwt_service.clone()))
            .app_data(web::Data::new(oauth_state.clone()))
            .wrap(rate_limiter.clone())
            .configure(|cfg| http::configure(cfg, oauth_state.clone()))
    })
    .bind((config.host, config.port))?
    .run()
    .await
}
