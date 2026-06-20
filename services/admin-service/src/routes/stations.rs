use actix_web::{delete, get, patch, post, web, HttpResponse};
use sqlx::PgPool;

use crate::db::stations as db_stations;
use crate::error::AppError;
use crate::models::station::{CreateStationRequest, UpdateStationRequest};

#[post("/stations")]
pub async fn create(
    pool: web::Data<PgPool>,
    body: web::Json<CreateStationRequest>,
) -> Result<HttpResponse, AppError> {
    let req = body.into_inner();
    if req.name.trim().is_empty() {
        return Err(AppError::ValidationError("Name is required".into()));
    }
    if !(-90.0..=90.0).contains(&req.latitude) {
        return Err(AppError::ValidationError(
            "Latitude must be between -90 and 90".into(),
        ));
    }
    if !(-180.0..=180.0).contains(&req.longitude) {
        return Err(AppError::ValidationError(
            "Longitude must be between -180 and 180".into(),
        ));
    }
    let station = db_stations::insert(pool.get_ref(), &req).await?;
    Ok(HttpResponse::Created().json(station))
}

#[get("/stations")]
pub async fn list(pool: web::Data<PgPool>) -> Result<HttpResponse, AppError> {
    let stations = db_stations::select_all(pool.get_ref()).await?;
    Ok(HttpResponse::Ok().json(stations))
}

#[get("/stations/{id}")]
pub async fn get_by_id(
    pool: web::Data<PgPool>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let station = db_stations::select_by_id(pool.get_ref(), &id).await?;
    Ok(HttpResponse::Ok().json(station))
}

#[patch("/stations/{id}")]
pub async fn update(
    pool: web::Data<PgPool>,
    path: web::Path<String>,
    body: web::Json<UpdateStationRequest>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let req = body.into_inner();
    if let Some(lat) = req.latitude {
        if !(-90.0..=90.0).contains(&lat) {
            return Err(AppError::ValidationError(
                "Latitude must be between -90 and 90".into(),
            ));
        }
    }
    if let Some(lon) = req.longitude {
        if !(-180.0..=180.0).contains(&lon) {
            return Err(AppError::ValidationError(
                "Longitude must be between -180 and 180".into(),
            ));
        }
    }
    let station = db_stations::update(pool.get_ref(), &id, &req).await?;
    Ok(HttpResponse::Ok().json(station))
}

#[delete("/stations/{id}")]
pub async fn delete(
    pool: web::Data<PgPool>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    db_stations::soft_delete(pool.get_ref(), &id).await?;
    Ok(HttpResponse::NoContent().finish())
}
