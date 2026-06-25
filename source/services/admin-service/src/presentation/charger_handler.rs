use axum::{extract::{Path, Query, State}, Json};
use axum::http::StatusCode;
use std::sync::Arc;
use serde::Deserialize;
use serde_json::{json, Value};
use crate::presentation::routes::AppState;
use crate::domain::charger::{CreateChargerRequest, UpdateChargerRequest};
use super::dto::{ChargerResponse, PaginatedResponse, PaginationParams, error_response};

pub async fn create_charger(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateChargerRequest>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    match state.charger_uc.create(req).await {
        Ok(c) => Ok((StatusCode::CREATED, Json(json!(ChargerResponse::from(c))))),
        Err(e) => Err(error_response(e)),
    }
}

pub async fn get_charger(
    State(state): State<Arc<AppState>>,
    Path(charger_id): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    match state.charger_uc.get(&charger_id).await {
        Ok(c) => Ok(Json(json!(ChargerResponse::from(c)))),
        Err(e) => Err(error_response(e)),
    }
}

#[derive(Deserialize)]
pub struct ListChargerParams {
    pub page: Option<i64>,
    pub per_page: Option<i64>,
    pub station_id: Option<String>,
}

pub async fn list_chargers(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ListChargerParams>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let pagination = PaginationParams { page: params.page, per_page: params.per_page };
    let page = pagination.page();
    let per_page = pagination.per_page();
    match state.charger_uc.list(page, per_page, params.station_id.as_deref()).await {
        Ok((chargers, total)) => {
            let data: Vec<ChargerResponse> = chargers.into_iter().map(Into::into).collect();
            Ok(Json(json!(PaginatedResponse::new(data, total, page, per_page))))
        }
        Err(e) => Err(error_response(e)),
    }
}

pub async fn update_charger(
    State(state): State<Arc<AppState>>,
    Path(charger_id): Path<String>,
    Json(req): Json<UpdateChargerRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    match state.charger_uc.update(&charger_id, req).await {
        Ok(c) => Ok(Json(json!(ChargerResponse::from(c)))),
        Err(e) => Err(error_response(e)),
    }
}

pub async fn delete_charger(
    State(state): State<Arc<AppState>>,
    Path(charger_id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<Value>)> {
    match state.charger_uc.delete(&charger_id).await {
        Ok(_) => Ok(StatusCode::NO_CONTENT),
        Err(e) => Err(error_response(e)),
    }
}
