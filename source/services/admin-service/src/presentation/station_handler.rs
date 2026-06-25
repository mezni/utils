use axum::{extract::{Path, Query, State}, Json};
use axum::http::StatusCode;
use std::sync::Arc;
use serde::Deserialize;
use serde_json::{json, Value};
use crate::presentation::routes::AppState;
use crate::domain::station::{CreateStationRequest, UpdateStationRequest};
use super::dto::{StationResponse, PaginatedResponse, PaginationParams, error_response};

pub async fn create_station(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateStationRequest>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    match state.station_uc.create(req).await {
        Ok(s) => Ok((StatusCode::CREATED, Json(json!(StationResponse::from(s))))),
        Err(e) => Err(error_response(e)),
    }
}

pub async fn get_station(
    State(state): State<Arc<AppState>>,
    Path(station_id): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    match state.station_uc.get(&station_id).await {
        Ok(s) => Ok(Json(json!(StationResponse::from(s)))),
        Err(e) => Err(error_response(e)),
    }
}

#[derive(Deserialize)]
pub struct ListStationParams {
    pub page: Option<i64>,
    pub per_page: Option<i64>,
    pub partner_id: Option<String>,
}

pub async fn list_stations(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ListStationParams>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let pagination = PaginationParams { page: params.page, per_page: params.per_page };
    let page = pagination.page();
    let per_page = pagination.per_page();
    match state.station_uc.list(page, per_page, params.partner_id.as_deref()).await {
        Ok((stations, total)) => {
            let data: Vec<StationResponse> = stations.into_iter().map(Into::into).collect();
            Ok(Json(json!(PaginatedResponse::new(data, total, page, per_page))))
        }
        Err(e) => Err(error_response(e)),
    }
}

pub async fn update_station(
    State(state): State<Arc<AppState>>,
    Path(station_id): Path<String>,
    Json(req): Json<UpdateStationRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    match state.station_uc.update(&station_id, req).await {
        Ok(s) => Ok(Json(json!(StationResponse::from(s)))),
        Err(e) => Err(error_response(e)),
    }
}

pub async fn delete_station(
    State(state): State<Arc<AppState>>,
    Path(station_id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<Value>)> {
    match state.station_uc.delete(&station_id).await {
        Ok(_) => Ok(StatusCode::NO_CONTENT),
        Err(e) => Err(error_response(e)),
    }
}
