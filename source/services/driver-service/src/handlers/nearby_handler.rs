use actix_web::{web, HttpResponse};
use serde_json::json;

use crate::dto::error_response::ApiResponse;
use crate::dto::nearby_query::NearbyQuery;
use crate::dto::station_response::StationResponse;
use crate::errors::app_error::AppError;

pub async fn find_nearby(
    pool: web::Data<sqlx::PgPool>,
    query: web::Query<NearbyQuery>,
) -> Result<HttpResponse, AppError> {
    query.validate()?;

    let stations = borne_data::find_nearby(pool.get_ref(), query.lat, query.lng, query.radius_m)
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
