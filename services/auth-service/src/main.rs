use actix_web::web;
use actix_web::{App, HttpServer};
use auth_service::{config, http, infrastructure};
use bornemap_db::{AppState, create_pool, run_migrations};
use config::AppConfig;
use infrastructure::jwt::JwtService;
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
            .configure(http::configure)
    })
    .bind((config.host, config.port))?
    .run()
    .await
}
