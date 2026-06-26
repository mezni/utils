use anyhow::Result;
use axum::Router;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use tracing::{error, info};

use crate::bootstrap::bootstrap;

mod application;
mod domain;
mod infrastructure;
mod presentation;
mod bootstrap;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "auth_service=debug,tower_http=debug,axum=debug".into())
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_thread_ids(true)
        )
        .init();

    info!("Starting BorneMap Authentication Service...");

    // Bootstrap the application
    let app_state = bootstrap().await?;

    // Create HTTP server
    let app = Router::new();

    // Add middlewares
    let app = app.layer(axum::middleware::from_fn(
        presentation::middleware::auth_middleware
    ));

    // Add API routes
    let app = crate::presentation::create_router(app_state.clone()).into();

    // Start server
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    info!("Server listening on http://0.0.0.0:8080");

    axum::serve(listener, app).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_bootstrap() {
        // This test would require proper environment setup
        let result = bootstrap().await;
        assert!(result.is_ok());
    }
}