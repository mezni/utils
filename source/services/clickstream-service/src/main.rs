use actix_web::web;
use sqlx::postgres::PgPoolOptions;
use tokio::signal;
use tracing::info;
use tracing_subscriber::EnvFilter;

use clickstream_service::config::Config;
use clickstream_service::db::repository::AnalyticsDbRepo;
use clickstream_service::middleware::rate_limiter;
use clickstream_service::routes;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .json()
        .init();

    let config = Config::from_env();

    info!("Connecting to analytics database");
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&config.database_url)
        .await
        .map_err(|e| {
            eprintln!("Failed to connect to analytics_db: {}", e);
            std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
        })?;

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .map_err(|e| {
            eprintln!("Migration failed: {}", e);
            std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
        })?;
    info!("Database migrations applied");

    let repo = AnalyticsDbRepo::new(pool);

    info!("Starting server at {}", config.bind_addr);

    let server = actix_web::HttpServer::new(move || {
        actix_web::App::new()
            .app_data(web::Data::new(repo.clone()))
            .wrap(rate_limiter::rate_limiter())
            .service(
                web::scope("/api/v1")
                    .route(
                        "/events",
                        web::post().to(routes::ingest::ingest_event),
                    )
                    .route(
                        "/events/batch",
                        web::post().to(routes::ingest::ingest_batch),
                    )
                    .route(
                        "/health",
                        web::get().to(routes::health::health_check),
                    ),
            )
    })
    .bind(&config.bind_addr)?
    .run();

    let handle = server.handle();
    let shutdown = async {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        info!("Shutdown signal received, stopping server...");
        handle.stop(true).await;
    };

    tokio::select! {
        result = server => result,
        _ = shutdown => Ok(()),
    }
}
