use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};
use std::net::SocketAddr;
use tracing_subscriber::EnvFilter;

async fn health() -> Json<Value> {
    Json(json!({"status":"ok"}))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // gis-worker is internal-only — no auth middleware needed.
    // Health endpoint is sufficient for liveness probes.
    let app = Router::new().route("/health", get(health));

    let port: u16 = std::env::var("GIS_WORKER_PORT")
        .unwrap_or_else(|_| "8084".into())
        .parse()
        .unwrap_or(8084);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("gis-worker listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
