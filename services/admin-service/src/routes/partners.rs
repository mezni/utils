use actix_web::{delete, get, patch, post, web, HttpResponse};
use sqlx::PgPool;

use crate::db::partners as db_partners;
use crate::error::AppError;
use crate::models::partner::{CreatePartnerRequest, UpdatePartnerRequest};

#[post("/partners")]
pub async fn create(
    pool: web::Data<PgPool>,
    body: web::Json<CreatePartnerRequest>,
) -> Result<HttpResponse, AppError> {
    let req = body.into_inner();
    if req.name.trim().is_empty() {
        return Err(AppError::ValidationError("Name is required".into()));
    }
    if !["INDIVIDUAL", "COMPANY"].contains(&req.network_type.as_str()) {
        return Err(AppError::ValidationError(format!(
            "Invalid network_type: {}. Must be INDIVIDUAL or COMPANY",
            req.network_type
        )));
    }
    let partner = db_partners::insert(pool.get_ref(), &req).await?;
    Ok(HttpResponse::Created().json(partner))
}

#[get("/partners")]
pub async fn list(pool: web::Data<PgPool>) -> Result<HttpResponse, AppError> {
    let partners = db_partners::select_all(pool.get_ref()).await?;
    Ok(HttpResponse::Ok().json(partners))
}

#[get("/partners/{id}")]
pub async fn get_by_id(
    pool: web::Data<PgPool>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let partner = db_partners::select_by_id(pool.get_ref(), &id).await?;
    Ok(HttpResponse::Ok().json(partner))
}

#[patch("/partners/{id}")]
pub async fn update(
    pool: web::Data<PgPool>,
    path: web::Path<String>,
    body: web::Json<UpdatePartnerRequest>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let req = body.into_inner();
    let partner = db_partners::update(pool.get_ref(), &id, &req).await?;
    Ok(HttpResponse::Ok().json(partner))
}

#[delete("/partners/{id}")]
pub async fn delete(
    pool: web::Data<PgPool>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    db_partners::soft_delete(pool.get_ref(), &id).await?;
    Ok(HttpResponse::NoContent().finish())
}
