use admin_service::{infrastructure, presentation};
use std::env;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env()
            .add_directive("admin_service=info".parse().unwrap()))
        .init();

    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");

    let pool = infrastructure::db::init_pool(&database_url)
        .await
        .expect("failed to connect to database");

    let partner_repo = infrastructure::repository::PartnerRepository::new(pool.clone());
    let station_repo = infrastructure::repository::StationRepository::new(pool.clone());
    let charger_repo = infrastructure::repository::ChargerRepository::new(pool);

    let app = presentation::routes::create_router(partner_repo, station_repo, charger_repo);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3002")
        .await
        .expect("failed to bind to port 3002");

    tracing::info!("admin-service starting on 0.0.0.0:3002");
    axum::serve(listener, app).await.unwrap();
}
