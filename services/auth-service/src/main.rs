use actix_web::web;
use actix_web::{App, HttpServer};
use auth_service::{config, http, infrastructure};
use bornemap_auth::{RedisOAuthStateStore, OAuthStateStore};
use bornemap_db::{AppState, create_pool, run_migrations};
use config::AppConfig;
use infrastructure::{PgOAuthRepository, GoogleOAuthProvider, OAuthState as OAuthAppState};
use tracing_subscriber::EnvFilter;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();

    let config = AppConfig::from_env().expect("configuration failed");

    let pool = create_pool(&config.database_url)
        .await
        .expect("DB connection failed");

    sqlx::query("SELECT 1")
        .execute(&pool)
        .await
        .expect("DB not reachable");

    run_migrations(&pool).await.expect("Migration failed");

    // Initialize OAuth components
    let oauth_state_store = Box::new(
        RedisOAuthStateStore::new(&config.redis_url)
            .expect("Failed to create OAuth state store")
    );
    
    let oauth_repository = PgOAuthRepository::new(pool.clone());
    
    let google_provider = config.google_client_id
        .and_then(|client_id| {
            config.google_client_secret.clone().map(|client_secret| {
                GoogleOAuthProvider::new(
                    client_id,
                    client_secret,
                    config.google_redirect_uri.unwrap_or_else(|| "http://localhost:8080/api/v1/auth/oauth/google/callback".to_string()),
                    config.google_auth_url,
                    config.google_token_url,
                    config.google_userinfo_url,
                )
            })
        });
    
    let oauth_state = OAuthAppState {
        google_provider,
        state_store: oauth_state_store,
        oauth_repository,
    };

    let state = AppState { db: pool };
    let jwt_service = JwtService::new(
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
            .configure(|cfg| http::configure(cfg, oauth_state.clone()))
    })
    .bind((config.host, config.port))?
    .run()
    .await
}
