use axum::{Json,Router,http::StatusCode};
use serde::Serialize;
use common_observability::SERVICE_VERSION;

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
    version: &'static str,
}

async fn health() -> (StatusCode, Json<HealthResponse>) {
    (
        StatusCode::OK,
        Json(HealthResponse {
            status: "ok",
            service: "driver-service",
            version: SERVICE_VERSION,
        }),
    )
}

#[tokio::main]
async fn main() {
    let port = std::env::var("SERVICE_PORT").unwrap_or_else(|_| "3001".into());
    let addr = format!("0.0.0.0:{port}");

    let app = Router::new().route("/health", axum::routing::get(health));

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    println!("driver-service listening on {addr}");
    axum::serve(listener, app).await.unwrap();
}
