use actix_web::web;
use tokio::signal;
use tracing::info;
use tracing_subscriber::EnvFilter;

use driver_service::{api, config, telemetry};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let settings = config::settings::Settings::from_env();

    info!(
        "Connecting to database at {}:{}",
        settings.db_host, settings.db_port
    );

    let pool = match borne_data::create_pool().await {
        Ok(p) => {
            info!("Database connection established");
            p
        }
        Err(e) => {
            info!("Starting without database connection: {}", e);
            std::process::exit(1);
        }
    };

    info!(
        "Starting server at {}:{}",
        settings.server_host, settings.server_port
    );

    let server = actix_web::HttpServer::new(move || {
        actix_web::App::new()
            .app_data(web::Data::new(pool.clone()))
            .wrap(actix_web::middleware::Logger::default())
            .wrap(actix_web::middleware::from_fn(
                telemetry::middleware::logging_middleware,
            ))
            .service(
                web::scope("/api/v1")
                    .service(web::scope("/stations").configure(api::v1::stations::configure))
                    .service(web::scope("/health").configure(api::v1::health::configure)),
            )
    })
    .bind(format!("{}:{}", settings.server_host, settings.server_port))?
    .run();

    let handle = server.handle();

    let shutdown = async {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        info!("Shutdown signal received, starting graceful shutdown...");
        handle.stop(true).await;
    };

    tokio::select! {
        result = server => result,
        _ = shutdown => Ok(()),
    }
}
