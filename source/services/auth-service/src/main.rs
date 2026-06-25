use std::sync::Arc;
use tracing_subscriber::EnvFilter;

use auth_service::infrastructure::{config::Config, database, keycloak::JwtValidator};
use auth_service::repository::user_profile_repository::UserProfileRepository;
use auth_service::router::create_router;
use auth_service::services::profile_service::ProfileService;
use auth_service::state::AppState;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("auth_service=info".parse().unwrap()),
        )
        .init();

    let config = Config::from_env();

    let pool = database::init_pool(&config.database_url)
        .await
        .expect("failed to connect to database");

    let keycloak = JwtValidator::new(&config.keycloak_jwks_url, &config.keycloak_issuer)
        .await
        .expect("failed to initialize JWT validator");

    let profile_repo = UserProfileRepository::new(pool.clone());
    let profile_service = ProfileService::new(profile_repo);

    let state = Arc::new(AppState {
        pool,
        keycloak,
        profile_service,
    });

    let app = create_router(state);

    let addr = format!("0.0.0.0:{}", config.service_port);
    tracing::info!("auth-service starting on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("failed to bind to port");

    axum::serve(listener, app).await.unwrap();
}
