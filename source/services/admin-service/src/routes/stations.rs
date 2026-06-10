use crate::config;
use crate::db;
use crate::error::AppError;
use crate::models::{CreateStationRequest, StationListParams, UpdateStationRequest};
use crate::AppState;
use actix_web::{delete, get, post, put, web, HttpRequest, HttpResponse};

#[post("/api/stations")]
pub async fn create_station(
    req: HttpRequest,
    state: web::Data<AppState>,
    body: web::Json<CreateStationRequest>,
) -> Result<HttpResponse, AppError> {
    let actor = config::x_partner_id(&req);
    if body.name.trim().is_empty() || body.name.len() > 255 {
        return Err(AppError::ValidationError(
            "name must be 1-255 characters".to_string(),
        ));
    }
    if !(-90.0..=90.0).contains(&body.latitude) {
        return Err(AppError::ValidationError(
            "latitude must be between -90 and 90".to_string(),
        ));
    }
    if !(-180.0..=180.0).contains(&body.longitude) {
        return Err(AppError::ValidationError(
            "longitude must be between -180 and 180".to_string(),
        ));
    }
    let station = db::stations::create_station(&state.pool, body.into_inner(), &actor).await?;
    Ok(HttpResponse::Created().json(station))
}

#[get("/api/stations")]
pub async fn list_stations(
    state: web::Data<AppState>,
    query: web::Query<StationListParams>,
) -> Result<HttpResponse, AppError> {
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(20).clamp(1, 100);
    let partner_id = query.partner_id.as_deref();
    let result = db::stations::list_stations(&state.pool, partner_id, page, page_size).await?;
    Ok(HttpResponse::Ok().json(result))
}

#[get("/api/stations/{id}")]
pub async fn get_station(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let station = db::stations::get_station(&state.pool, &id).await?;
    Ok(HttpResponse::Ok().json(station))
}

#[put("/api/stations/{id}")]
pub async fn update_station(
    req: HttpRequest,
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<UpdateStationRequest>,
) -> Result<HttpResponse, AppError> {
    let actor = config::x_partner_id(&req);
    let id = path.into_inner();
    if let Some(ref name) = body.name
        && (name.trim().is_empty() || name.len() > 255)
    {
        return Err(AppError::ValidationError(
            "name must be 1-255 characters".to_string(),
        ));
    }
    if let Some(lat) = body.latitude
        && !(-90.0..=90.0).contains(&lat)
    {
        return Err(AppError::ValidationError(
            "latitude must be between -90 and 90".to_string(),
        ));
    }
    if let Some(lng) = body.longitude
        && !(-180.0..=180.0).contains(&lng)
    {
        return Err(AppError::ValidationError(
            "longitude must be between -180 and 180".to_string(),
        ));
    }
    let station = db::stations::update_station(&state.pool, &id, body.into_inner(), &actor).await?;
    Ok(HttpResponse::Ok().json(station))
}

#[delete("/api/stations/{id}")]
pub async fn delete_station(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    db::stations::delete_station(&state.pool, &id).await?;
    Ok(HttpResponse::Ok().json(serde_json::json!({"deleted": true})))
}
