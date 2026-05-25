use actix_web::{web, App, HttpResponse, HttpServer, Responder, get};
use tracing_subscriber::EnvFilter;

mod domain;
mod utils;

#[get("/api/v1/health")]
async fn health() -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "ok",
        "service": "bornemap-backend"
    }))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    tracing::info!("Starting BorneMap backend on :8080");

    HttpServer::new(|| {
        App::new()
            .service(health)
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await
}
