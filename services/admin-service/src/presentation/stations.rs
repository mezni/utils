use actix_web::{web, HttpResponse};
use std::sync::Arc;

use crate::application::station_service::StationService;
use bornemap_platform_core::constants::{DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, MIN_PAGE_SIZE};
use bornemap_platform_core::pagination::validate_limit;
use bornemap_platform_core::result::{ApiResponse, to_error_response, ErrorResponse};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct ListQuery {
    page: Option<u32>,
    limit: Option<u32>,
    partner_id: Option<String>,
}

#[derive(Deserialize)]
pub struct CreateBody {
    name: String,
    location: Option<String>,
    partner_id: String,
}

#[derive(Deserialize)]
pub struct UpdateBody {
    name: String,
    location: Option<String>,
}

#[derive(Serialize)]
pub struct ListData<T: Serialize> {
    items: Vec<T>,
    pagination: Pagination,
}

pub async fn list(
    query: web::Query<ListQuery>,
    service: web::Data<Arc<StationService>>,
) -> HttpResponse {
    let limit = validate_limit(query.limit.unwrap_or(DEFAULT_PAGE_SIZE));
    let page = query.page.unwrap_or(1).clamp(1, u32::MAX);
    let partner_id = query.partner_id.as_deref();

    match service.list(page, limit, partner_id).await {
        Ok((items, total)) => {
            let data = ListData {
                items,
                pagination: Pagination::new(page, limit, total),
            };
            HttpResponse::Ok().json(ApiResponse::ok(data))
        }
        Err(e) => {
            let resp = to_error_response(&e);
            HttpResponse::InternalServerError().json(resp)
        }
    }
}

pub async fn get(
    path: web::Path<String>,
    service: web::Data<Arc<StationService>>,
) -> HttpResponse {
    let id = path.into_inner();
    match service.get(&id).await {
        Ok(station) => HttpResponse::Ok().json(ApiResponse::ok(station)),
        Err(e) => {
            let resp = to_error_response(&e);
            let status = match &e {
                bornemap_platform_core::error::AppError::NotFound(_) => actix_web::http::StatusCode::NOT_FOUND,
                _ => actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
            };
            HttpResponse::build(status).json(resp)
        }
    }
}

pub async fn create(
    body: web::Json<CreateBody>,
    service: web::Data<Arc<StationService>>,
) -> HttpResponse {
    match service.create(&body.name, body.location.as_deref(), &body.partner_id, "admin-user-id").await {
        Ok(station) => HttpResponse::Created().json(ApiResponse::ok(station)),
        Err(e) => {
            let resp = to_error_response(&e);
            let status = match &e {
                bornemap_platform_core::error::AppError::Validation(_) => actix_web::http::StatusCode::BAD_REQUEST,
                _ => actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
            };
            HttpResponse::build(status).json(resp)
        }
    }
}

pub async fn update(
    path: web::Path<String>,
    body: web::Json<UpdateBody>,
    service: web::Data<Arc<StationService>>,
) -> HttpResponse {
    let id = path.into_inner();
    match service.update(&id, &body.name, body.location.as_deref(), "admin-user-id").await {
        Ok(station) => HttpResponse::Ok().json(ApiResponse::ok(station)),
        Err(e) => {
            let resp = to_error_response(&e);
            let status = match &e {
                bornemap_platform_core::error::AppError::NotFound(_) => actix_web::http::StatusCode::NOT_FOUND,
                bornemap_platform_core::error::AppError::Validation(_) => actix_web::http::StatusCode::BAD_REQUEST,
                _ => actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
            };
            HttpResponse::build(status).json(resp)
        }
    }
}

pub async fn delete(
    path: web::Path<String>,
    service: web::Data<Arc<StationService>>,
) -> HttpResponse {
    let id = path.into_inner();
    match service.hard_delete(&id).await {
        Ok(_) => HttpResponse::Ok().json(ApiResponse::ok(())),
        Err(e) => {
            let resp = to_error_response(&e);
            let status = match &e {
                bornemap_platform_core::error::AppError::NotFound(_) => actix_web::http::StatusCode::NOT_FOUND,
                bornemap_platform_core::error::AppError::Validation(_) => actix_web::http::StatusCode::BAD_REQUEST,
                _ => actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
            };
            HttpResponse::build(status).json(resp)
        }
    }
}
