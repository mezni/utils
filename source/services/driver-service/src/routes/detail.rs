use actix_web::{get, web, HttpResponse};
use crate::error::AppError;
use crate::AppState;

#[get("/api/stations/{id}")]
pub async fn detail(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let station_id = path.into_inner();
    let station = crate::db::detail::get_station(&state.pool, &station_id).await?;
    Ok(HttpResponse::Ok().json(station))
}
