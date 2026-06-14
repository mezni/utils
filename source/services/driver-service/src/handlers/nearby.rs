use axum::extract::{Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::Value;

use crate::error::DomainError;
use crate::repositories::StationRepositoryImpl;
use crate::services::StationService;
use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct NearbyParams {
    pub lat: Option<f64>,
    pub lng: Option<f64>,
    pub radius: Option<f64>,
}

pub async fn nearby_search(
    State(state): State<AppState>,
    Query(params): Query<NearbyParams>,
) -> Result<Json<Value>, DomainError> {
    let lat = params
        .lat
        .ok_or_else(|| DomainError::BadRequest("Parameter 'lat' is required".into()))?;
    let lng = params
        .lng
        .ok_or_else(|| DomainError::BadRequest("Parameter 'lng' is required".into()))?;
    let radius = params.radius.unwrap_or(5000.0);

    let repo = StationRepositoryImpl::new(state.db);
    let service = StationService::new(repo);
    let stations = service.find_nearby(lat, lng, radius).await?;
    Ok(Json(serde_json::to_value(&stations).unwrap()))
}
