use actix_web::{App, HttpServer, middleware as actix_middleware};
use tracing_subscriber::EnvFilter;

mod config;
mod errors;
mod domain;
mod application;
mod infrastructure;
mod interface;

async fn run_migrations(database_url: &str) -> Result<(), sqlx::migrate::MigrateError> {
    tracing::info!("Running database migrations...");

    let pool = sqlx::PgPool::connect(database_url).await
        .map_err(|e| sqlx::migrate::MigrateError::Source(e.into()))?;

    // Run migrations from the db/migrations directory
    let migrations = sqlx::migrate::Migrator::new(std::path::Path::new("db/migrations"))
        .await?;

    migrations.run(&pool).await?;

    tracing::info!("Database migrations complete");
    Ok(())
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let app_config = config::AppConfig::from_env()
        .expect("Failed to load configuration from environment");

    tracing::info!("Starting Driver Service on {}", app_config.bind_address());

    // Run migrations
    match run_migrations(&app_config.database_url).await {
        Ok(()) => {
            tracing::info!("Migrations ran successfully");
        }
        Err(e) => {
            tracing::warn!("Could not run migrations: {}", e);
            tracing::warn!("Service will start without database migrations");
        }
    }

    // Attempt database connection
    match infrastructure::db::pool::create_pool(&app_config.database_url).await {
        Ok(pool) => {
            tracing::info!("Successfully connected to PostgreSQL");
            let _ = pool;
        }
        Err(e) => {
            tracing::warn!("Could not connect to PostgreSQL on startup: {}", e);
            tracing::warn!("Service will start without database connection (health endpoint only)");
        }
    }

    let bind_addr = app_config.bind_address();

    HttpServer::new(move || {
        App::new()
            .wrap(actix_middleware::Logger::default())
            .wrap(actix_middleware::Compress::default())
            .configure(interface::router::configure)
    })
    .bind(&bind_addr)?
    .run()
    .await
}
