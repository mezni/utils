use actix_web::{get, web, HttpResponse};
use crate::error::AppError;
use crate::AppState;

#[get("/api/stations/nearby")]
pub async fn nearby(
    state: web::Data<AppState>,
    query: web::Query<NearbyQuery>,
) -> Result<HttpResponse, AppError> {
    let params = query.into_inner();

    if !(-90.0..=90.0).contains(&params.lat) {
        return Err(AppError::BadRequest("lat must be between -90 and 90".to_string()));
    }
    if !(-180.0..=180.0).contains(&params.lng) {
        return Err(AppError::BadRequest("lng must be between -180 and 180".to_string()));
    }
    if params.radius > 500_000.0 {
        return Err(AppError::BadRequest("radius must not exceed 500000 meters".to_string()));
    }
    if params.limit > 100 {
        return Err(AppError::BadRequest("limit must not exceed 100".to_string()));
    }

    let stations = crate::db::nearby::nearby_stations(
        &state.pool,
        params.lat,
        params.lng,
        params.radius,
        params.limit,
        params.offset,
    )
    .await?;

    Ok(HttpResponse::Ok().json(stations))
}

#[derive(serde::Deserialize)]
pub struct NearbyQuery {
    pub lat: f64,
    pub lng: f64,
    #[serde(default = "default_radius")]
    pub radius: f64,
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

fn default_radius() -> f64 {
    10_000.0
}

fn default_limit() -> i64 {
    20
}
