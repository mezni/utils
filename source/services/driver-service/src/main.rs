mod domain;
mod application;
mod infrastructure;
mod presentation;

use std::env;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env()
            .add_directive("driver_service=info".parse().unwrap()))
        .init();

    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");

    let pool = infrastructure::db::init_pool(&database_url)
        .await
        .expect("failed to connect to database");

    let repository = infrastructure::repository::PgStationRepository::new(pool);
    let app = presentation::routes::create_router(repository);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001")
        .await
        .expect("failed to bind to port 3001");

    tracing::info!("driver-service starting on 0.0.0.0:3001");
    axum::serve(listener, app).await.unwrap();
}
