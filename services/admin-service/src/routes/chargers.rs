use actix_web::{delete, get, patch, post, web, HttpResponse};
use sqlx::PgPool;

use crate::db::chargers as db_chargers;
use crate::error::AppError;
use crate::models::charger::{CreateChargerRequest, UpdateChargerRequest};

#[post("/chargers")]
pub async fn create(
    pool: web::Data<PgPool>,
    body: web::Json<CreateChargerRequest>,
) -> Result<HttpResponse, AppError> {
    let req = body.into_inner();
    if req.count_total.unwrap_or(1) < 1 {
        return Err(AppError::ValidationError(
            "count_total must be at least 1".into(),
        ));
    }
    if req.count_available.unwrap_or(1) < 0 {
        return Err(AppError::ValidationError(
            "count_available must be non-negative".into(),
        ));
    }
    if req.station_id.is_empty() {
        return Err(AppError::ValidationError("station_id is required".into()));
    }
    let charger = db_chargers::insert(pool.get_ref(), &req).await?;
    Ok(HttpResponse::Created().json(charger))
}

#[get("/chargers")]
pub async fn list(
    pool: web::Data<PgPool>,
    query: web::Query<std::collections::HashMap<String, String>>,
) -> Result<HttpResponse, AppError> {
    let station_id = query.get("station_id").map(|s| s.as_str());
    let chargers = db_chargers::select_all(pool.get_ref(), station_id).await?;
    Ok(HttpResponse::Ok().json(chargers))
}

#[get("/chargers/{id}")]
pub async fn get_by_id(
    pool: web::Data<PgPool>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let charger = db_chargers::select_by_id(pool.get_ref(), &id).await?;
    Ok(HttpResponse::Ok().json(charger))
}

#[patch("/chargers/{id}")]
pub async fn update(
    pool: web::Data<PgPool>,
    path: web::Path<String>,
    body: web::Json<UpdateChargerRequest>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let req = body.into_inner();
    let charger = db_chargers::update(pool.get_ref(), &id, &req).await?;
    Ok(HttpResponse::Ok().json(charger))
}

#[delete("/chargers/{id}")]
pub async fn delete(
    pool: web::Data<PgPool>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    db_chargers::soft_delete(pool.get_ref(), &id).await?;
    Ok(HttpResponse::NoContent().finish())
}
