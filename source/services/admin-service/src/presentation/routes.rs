use axum::{Router, routing::{get, post}};
use std::sync::Arc;
use axum::http::Method;
use tower_http::cors::{Any, CorsLayer};
use crate::infrastructure::repository::{PartnerRepository, StationRepository, ChargerRepository};
use crate::application::partner_use_cases::PartnerUseCases;
use crate::application::station_use_cases::StationUseCases;
use crate::application::charger_use_cases::ChargerUseCases;
use super::health::health_handler;
use super::partner_handler::{create_partner, get_partner, list_partners, update_partner, delete_partner};
use super::station_handler::{create_station, get_station, list_stations, update_station, delete_station};
use super::charger_handler::{create_charger, get_charger, list_chargers, update_charger, delete_charger};

pub struct AppState {
    pub partner_uc: Arc<PartnerUseCases>,
    pub station_uc: Arc<StationUseCases>,
    pub charger_uc: Arc<ChargerUseCases>,
}

pub fn create_router(
    partner_repo: PartnerRepository,
    station_repo: StationRepository,
    charger_repo: ChargerRepository,
) -> Router {
    let partner_uc = Arc::new(PartnerUseCases::new(partner_repo));
    let station_uc = Arc::new(StationUseCases::new(station_repo));
    let charger_uc = Arc::new(ChargerUseCases::new(charger_repo));
    let state = Arc::new(AppState { partner_uc, station_uc, charger_uc });

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE, Method::OPTIONS])
        .allow_headers(Any);

    Router::new()
        .route("/api/v1/health", get(health_handler))
        .route("/api/v1/partners", post(create_partner).get(list_partners))
        .route("/api/v1/partners/{partner_id}", get(get_partner).put(update_partner).delete(delete_partner))
        .route("/api/v1/stations", post(create_station).get(list_stations))
        .route("/api/v1/stations/{station_id}", get(get_station).put(update_station).delete(delete_station))
        .route("/api/v1/chargers", post(create_charger).get(list_chargers))
        .route("/api/v1/chargers/{charger_id}", get(get_charger).put(update_charger).delete(delete_charger))
        .with_state(state)
        .layer(cors)
}
