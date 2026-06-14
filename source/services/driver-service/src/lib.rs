pub mod config;
pub mod error;
pub mod handlers;
pub mod models;
pub mod repositories;
pub mod services;

use axum::Router;
use sqlx::PgPool;

use crate::handlers::{health, nearby, stations};

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
}

pub async fn build_router(db: PgPool) -> Router {
    let state = AppState { db };

    Router::new()
        .route("/api/v1/health", axum::routing::get(health::health_check))
        .route("/api/v1/stations", axum::routing::get(stations::list_stations))
        .route(
            "/api/v1/stations/{id}",
            axum::routing::get(stations::get_station),
        )
        .route(
            "/api/v1/stations/nearby",
            axum::routing::get(nearby::nearby_search),
        )
        .with_state(state)
}
