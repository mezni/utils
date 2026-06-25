use std::sync::Arc;

use auth_service::infrastructure::config::Config;
use auth_service::infrastructure::database;
use auth_service::infrastructure::keycloak::JwtValidator;
use auth_service::repository::user_profile_repository::UserProfileRepository;
use auth_service::router::create_router;
use auth_service::services::profile_service::ProfileService;
use auth_service::state::AppState;
use auth_service::oidc::client::OidcClient;
use auth_service::session::manager::SessionManager;
use tracing_subscriber::EnvFilter;

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

    let keycloak = JwtValidator::new(&config.oidc.jwks_url, &config.oidc.issuer)
        .await
        .expect("failed to initialize JWT validator");

    let profile_repo = UserProfileRepository::new(pool.clone());
    let profile_service = ProfileService::new(profile_repo);

    let oidc_client = OidcClient::new(&config.oidc)
        .expect("failed to initialize OIDC client");

    let session_manager = SessionManager::new();

    let state = Arc::new(AppState {
        pool,
        keycloak,
        profile_service,
        oidc_client,
        session_manager,
    });

    let app = create_router(state);

    let addr = format!("0.0.0.0:{}", config.service_port);
    tracing::info!("auth-service starting on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("failed to bind to port");

    axum::serve(listener, app).await.unwrap();
}
