use axum::extract::{Path, State};
use axum::Json;
use serde_json::Value;

use crate::error::DomainError;
use crate::repositories::StationRepositoryImpl;
use crate::services::StationService;
use crate::AppState;

pub async fn list_stations(
    State(state): State<AppState>,
) -> Result<Json<Value>, DomainError> {
    let repo = StationRepositoryImpl::new(state.db);
    let service = StationService::new(repo);
    let stations = service.list_stations().await?;
    Ok(Json(serde_json::to_value(&stations).unwrap()))
}

pub async fn get_station(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, DomainError> {
    if id.is_empty() {
        return Err(DomainError::BadRequest("Invalid station ID format".into()));
    }

    let repo = StationRepositoryImpl::new(state.db);
    let service = StationService::new(repo);
    let station = service.get_station(&id).await?;
    Ok(Json(serde_json::to_value(&station).unwrap()))
}
