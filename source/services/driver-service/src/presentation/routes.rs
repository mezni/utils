use axum::{Router, routing::get};
use std::sync::Arc;
use crate::infrastructure::repository::PgStationRepository;
use super::health::health_handler;
use super::nearby::nearby_handler;

pub struct AppState {
    pub use_case: crate::application::get_nearby_stations::GetNearbyStationsUseCase,
}

pub fn create_router(repository: PgStationRepository) -> Router {
    let use_case = crate::application::get_nearby_stations::GetNearbyStationsUseCase::new(repository);
    let state = Arc::new(AppState { use_case });

    Router::new()
        .route("/api/v1/health", get(health_handler))
        .route("/api/v1/stations/nearby", get(nearby_handler))
        .with_state(state)
}
