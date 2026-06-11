use actix_web::{web, HttpResponse};
use serde_json::json;

use crate::dto::error_response::ApiResponse;
use crate::dto::station_response::StationResponse;
use crate::errors::app_error::AppError;

pub async fn list_stations(pool: web::Data<sqlx::PgPool>) -> Result<HttpResponse, AppError> {
    let stations = borne_data::list_all(pool.get_ref())
        .await
        .map_err(AppError::from)?;

    let limited: Vec<StationResponse> = stations.into_iter().take(100).map(Into::into).collect();
    let count = limited.len();

    let response = ApiResponse {
        data: Some(limited),
        error: None,
        meta: Some(json!({ "count": count })),
    };

    Ok(HttpResponse::Ok().json(response))
}

pub async fn get_station_detail(
    pool: web::Data<sqlx::PgPool>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let detail = borne_data::find_by_id(pool.get_ref(), &id)
        .await
        .map_err(AppError::from)?;

    use crate::dto::station_detail_response::{
        ChargerResponse, PartnerResponse, StationDetailResponse,
    };

    let response = StationDetailResponse {
        id: detail.station.id,
        name: detail.station.name,
        address: detail.station.address,
        latitude: detail.station.latitude,
        longitude: detail.station.longitude,
        chargers: detail
            .chargers
            .into_iter()
            .map(ChargerResponse::from)
            .collect(),
        partner: PartnerResponse::from(detail.partner),
    };

    let api_response = ApiResponse {
        data: Some(response),
        error: None,
        meta: None,
    };

    Ok(HttpResponse::Ok().json(api_response))
}
