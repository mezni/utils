use actix_web::{get, web, HttpResponse};
use crate::error::AppError;
use crate::AppState;

#[get("/api/stations/markers")]
pub async fn markers(
    state: web::Data<AppState>,
    query: web::Query<MarkersQuery>,
) -> Result<HttpResponse, AppError> {
    let params = query.into_inner();

    if params.south >= params.north {
        return Err(AppError::BadRequest("south must be less than north".to_string()));
    }
    if params.west >= params.east {
        return Err(AppError::BadRequest("west must be less than east".to_string()));
    }

    let stations = crate::db::markers::markers_in_bbox(
        &state.pool,
        params.south,
        params.west,
        params.north,
        params.east,
    )
    .await?;

    Ok(HttpResponse::Ok().json(stations))
}

#[derive(serde::Deserialize)]
pub struct MarkersQuery {
    pub south: f64,
    pub west: f64,
    pub north: f64,
    pub east: f64,
}
