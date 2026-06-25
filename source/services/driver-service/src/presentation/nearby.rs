use axum::{extract::{Query, State}, Json};
use axum::http::StatusCode;
use serde::Deserialize;
use std::sync::Arc;
use serde_json::json;
use crate::application::get_nearby_stations::NearbyQuery;
use super::routes::AppState;
use super::dto::NearbyStationResponse;

#[derive(Deserialize)]
pub struct NearbyParams {
    lat: Option<f64>,
    lon: Option<f64>,
    radius: Option<i32>,
    limit: Option<i32>,
}

pub async fn nearby_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<NearbyParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let lat = params.lat.ok_or_else(|| {
        (StatusCode::BAD_REQUEST, Json(json!({"error": "missing required parameter: lat"})))
    })?;

    let lon = params.lon.ok_or_else(|| {
        (StatusCode::BAD_REQUEST, Json(json!({"error": "missing required parameter: lon"})))
    })?;

    if !(-90.0..=90.0).contains(&lat) {
        return Err((StatusCode::BAD_REQUEST, Json(json!({"error": "lat must be between -90 and 90"}))));
    }
    if !(-180.0..=180.0).contains(&lon) {
        return Err((StatusCode::BAD_REQUEST, Json(json!({"error": "lon must be between -180 and 180"}))));
    }

    if let Some(r) = params.radius {
        if r <= 0 {
            return Err((StatusCode::BAD_REQUEST, Json(json!({"error": "radius must be positive"}))));
        }
    }

    if let Some(l) = params.limit {
        if !(1..=100).contains(&l) {
            return Err((StatusCode::BAD_REQUEST, Json(json!({"error": "limit must be between 1 and 100"}))));
        }
    }

    let query = NearbyQuery { lat, lon, radius: params.radius, limit: params.limit };

    match state.use_case.execute(query).await {
        Ok(stations) => {
            let data: Vec<NearbyStationResponse> = stations.into_iter().map(Into::into).collect();
            Ok(Json(json!({ "data": data })))
        }
        Err(e) => {
            tracing::error!(error = %e, "nearby query failed");
            Err((StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal server error"}))))
        }
    }
}
