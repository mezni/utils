use actix_web::{web, App, HttpServer, middleware::Compress};
use actix_cors::Cors;
use driver_service::{AppState, routes};
use std::time::Instant;
use tracing_actix_web::TracingLogger;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "driver_service=info,sqlx=warn".into()),
        )
        .init();

    dotenvy::dotenv().ok();

    let database_url = std::env::var("PLATFORM_DB_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .expect("PLATFORM_DB_URL or DATABASE_URL must be set");

    let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".into());
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);

    let db_pool = ev_db::pool::create_pool(&database_url, 10)
        .await
        .expect("Failed to connect to platform_db");

    let startup_time = Instant::now();
    let service_name = "driver-service".to_string();

    tracing::info!("Starting driver-service on {}:{}", host, port);

    let server = HttpServer::new(move || {
        App::new()
            .wrap(TracingLogger::default())
            .wrap(Compress::default())
            .wrap(Cors::permissive())
            .app_data(web::Data::new(AppState {
                db_pool: db_pool.clone(),
                startup_time,
                service_name: service_name.clone(),
            }))
            .configure(routes::configure)
    })
    .bind((host.as_str(), port))?
    .run();

    let handle = server.handle();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Shutdown signal received, draining connections...");
        handle.stop(true).await;
    });

    server.await
}
