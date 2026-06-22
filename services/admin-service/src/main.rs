use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct HealthResponse {
    status: String,
    timestamp: String,
    service: String,
}

async fn health() -> impl Responder {
    let response = HealthResponse {
        status: "ok".to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        service: "admin-service".to_string(),
    };
    HttpResponse::Ok().json(response)
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = config::Config::builder()
        .add_source(config::File::with_name("config.toml"))
        .build()
        .expect("Failed to build config");

    let port = config.get::<u16>("server.port").unwrap_or(3002);

    tracing::info!("Starting admin-service on port {}", port);

    HttpServer::new(|| {
        App::new()
            .route("/health", web::get().to(health))
    })
    .bind(("127.0.0.1", port))?
    .run()
    .await?;

    Ok(())
}
