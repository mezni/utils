use sqlx::migrate::Migrator;
use sqlx::postgres::PgPoolOptions;
use std::env;
use tracing::{error, info};

use shared_database::Database;
use shared_cache::Cache;
use shared_jwt::JwtService;
use crate::infrastructure::{DatabaseInfrastructure, CacheInfrastructure, JwtInfrastructure};
use crate::application::use_cases::{RegisterUserUseCase, LoginUserUseCase, RefreshTokenUseCase, LogoutUseCase};
use crate::application::repositories::{UserRepository, RefreshTokenRepository, AuditLogRepository};
use crate::presentation::{AppState, create_router};
use crate::domain::services::{PasswordService, TokenPolicyService};

pub async fn bootstrap() -> Result<AppState, anyhow::Error> {
    info!("Starting authentication service bootstrap...");

    // Load environment variables
    dotenvy::dotenv().ok();

    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:password@localhost/auth_db".to_string());
    let redis_url = env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let jwt_secret = env::var("JWT_SECRET")
        .expect("JWT_SECRET environment variable must be set");
    let jwt_issuer = env::var("JWT_ISSUER").unwrap_or_else(|_| "borne-map-auth".to_string());
    let app_base_url = env::var("APP_BASE_URL").unwrap_or_else(|_| "http://localhost:8080".to_string());

    // Initialize database
    info!("Connecting to database...");
    let db_infra = DatabaseInfrastructure::new(database_url.clone()).await?;
    let db_pool = db_infra.get_database().get_pool_ref().clone();

    // Run migrations
    info!("Running database migrations...");
    let migrator = Migrator::from_path("migrations")?;
    migrator.run(&db_pool).await?;

    // Initialize Redis
    info!("Connecting to Redis...");
    let cache_infra = CacheInfrastructure::new(Cache::from_connection_string(&redis_url).await?);

    // Initialize JWT service
    info!("Initializing JWT service...");
    let jwt_infra = JwtInfrastructure::new(&jwt_secret)?;

    // Create use cases
    info!("Creating use cases...");
    let user_repository = Box::new(db_infra);
    let refresh_token_repository = Box::new(db_infra);
    let audit_log_repository = Box::new(db_infra);

    let register_use_case = RegisterUserUseCase::new(
        user_repository,
        refresh_token_repository,
        audit_log_repository,
        jwt_infra.get_jwt_service().clone(),
        "dev_pepper".to_string(),
    );

    let login_use_case = LoginUserUseCase::new(
        user_repository,
        refresh_token_repository,
        audit_log_repository,
        jwt_infra.get_jwt_service().clone(),
        cache_infra.clone(),
        "dev_pepper".to_string(),
    );

    let refresh_use_case = RefreshTokenUseCase::new(
        user_repository,
        refresh_token_repository,
        audit_log_repository,
        jwt_infra.get_jwt_service().clone(),
        cache_infra.clone(),
        "dev_pepper".to_string(),
    );

    let logout_use_case = LogoutUseCase::new(
        user_repository,
        refresh_token_repository,
        audit_log_repository,
        jwt_infra.get_jwt_service().clone(),
        cache_infra,
    );

    // Create app state
    let app_state = Arc::new(AppState {
        database: db_infra,
        cache: cache_infra,
        jwt: jwt_infra,
        register_use_case,
        login_use_case,
        refresh_use_case,
        logout_use_case,
    });

    // Create router
    let router = create_router(app_state.clone());

    info!("Authentication service bootstrap completed successfully");

    Ok(app_state)
}