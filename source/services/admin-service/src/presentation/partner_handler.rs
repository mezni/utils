use axum::{extract::{Path, Query, State}, Json};
use axum::http::StatusCode;
use std::sync::Arc;
use serde::Deserialize;
use serde_json::{json, Value};
use crate::presentation::routes::AppState;
use crate::domain::partner::{CreatePartnerRequest, UpdatePartnerRequest};
use super::dto::{PartnerResponse, PaginatedResponse, PaginationParams, error_response};

pub async fn create_partner(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreatePartnerRequest>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    match state.partner_uc.create(req).await {
        Ok(p) => Ok((StatusCode::CREATED, Json(json!(PartnerResponse::from(p))))),
        Err(e) => Err(error_response(e)),
    }
}

pub async fn get_partner(
    State(state): State<Arc<AppState>>,
    Path(partner_id): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    match state.partner_uc.get(&partner_id).await {
        Ok(p) => Ok(Json(json!(PartnerResponse::from(p)))),
        Err(e) => Err(error_response(e)),
    }
}

#[derive(Deserialize)]
pub struct ListPartnerParams {
    pub page: Option<i64>,
    pub per_page: Option<i64>,
    pub search: Option<String>,
}

pub async fn list_partners(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ListPartnerParams>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let pagination = PaginationParams { page: params.page, per_page: params.per_page };
    let page = pagination.page();
    let per_page = pagination.per_page();
    match state.partner_uc.list(page, per_page, params.search.as_deref()).await {
        Ok((partners, total)) => {
            let data: Vec<PartnerResponse> = partners.into_iter().map(Into::into).collect();
            Ok(Json(json!(PaginatedResponse::new(data, total, page, per_page))))
        }
        Err(e) => Err(error_response(e)),
    }
}

pub async fn update_partner(
    State(state): State<Arc<AppState>>,
    Path(partner_id): Path<String>,
    Json(req): Json<UpdatePartnerRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    match state.partner_uc.update(&partner_id, req).await {
        Ok(p) => Ok(Json(json!(PartnerResponse::from(p)))),
        Err(e) => Err(error_response(e)),
    }
}

pub async fn delete_partner(
    State(state): State<Arc<AppState>>,
    Path(partner_id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<Value>)> {
    match state.partner_uc.delete(&partner_id).await {
        Ok(_) => Ok(StatusCode::NO_CONTENT),
        Err(e) => Err(error_response(e)),
    }
}
