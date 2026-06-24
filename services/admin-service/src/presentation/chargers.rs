use actix_web::{web, HttpResponse};
use std::sync::Arc;

use crate::application::charger_service::ChargerService;
use bornemap_platform_core::constants::{DEFAULT_PAGE_SIZE, MAX_POWER_RATING, MIN_POWER_RATING, MIN_PAGE_SIZE};
use bornemap_platform_core::pagination::validate_limit;
use bornemap_platform_core::result::{ApiResponse, to_error_response, ErrorResponse};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct ListQuery {
    page: Option<u32>,
    limit: Option<u32>,
    station_id: Option<String>,
}

#[derive(Deserialize)]
pub struct CreateBody {
    station_id: String,
    status: String,
    power_rating: i32,
}

#[derive(Deserialize)]
pub struct UpdatePowerRatingBody {
    power_rating: i32,
}

#[derive(Deserialize)]
pub struct UpdateStatusBody {
    status: String,
}

#[derive(Serialize)]
pub struct ListData<T: Serialize> {
    items: Vec<T>,
    pagination: Pagination,
}

pub async fn list(
    query: web::Query<ListQuery>,
    service: web::Data<Arc<ChargerService>>,
) -> HttpResponse {
    let limit = validate_limit(query.limit.unwrap_or(DEFAULT_PAGE_SIZE));
    let page = query.page.unwrap_or(1).clamp(1, u32::MAX);
    let station_id = query.station_id.as_deref();

    match service.list(page, limit, station_id).await {
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
    service: web::Data<Arc<ChargerService>>,
) -> HttpResponse {
    let id = path.into_inner();
    match service.get(&id).await {
        Ok(charger) => HttpResponse::Ok().json(ApiResponse::ok(charger)),
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
    service: web::Data<Arc<ChargerService>>,
) -> HttpResponse {
    match service.create(&body.station_id, &body.status, body.power_rating, "admin-user-id").await {
        Ok(charger) => HttpResponse::Created().json(ApiResponse::ok(charger)),
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
    service: web::Data<Arc<ChargerService>>,
) -> HttpResponse {
    let id = path.into_inner();
    let resp = to_error_response(&bornemap_platform_core::error::AppError::NotFound(format!("PATCH endpoint not implemented - use /status or /power_rating")));
    HttpResponse::BadRequest().json(resp)
}

pub async fn patch(
    path: web::Path<String>,
    body: web::Json<UpdatePowerRatingBody>,
    service: web::Data<Arc<ChargerService>>,
) -> HttpResponse {
    let id = path.into_inner();
    match service.update_power_rating(&id, body.power_rating, "admin-user-id").await {
        Ok(charger) => HttpResponse::Ok().json(ApiResponse::ok(charger)),
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

pub async fn update_status(
    path: web::Path<String>,
    body: web::Json<UpdateStatusBody>,
    service: web::Data<Arc<ChargerService>>,
) -> HttpResponse {
    let id = path.into_inner();
    match service.update_status(&id, &body.status, "admin-user-id").await {
        Ok(charger) => HttpResponse::Ok().json(ApiResponse::ok(charger)),
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

pub async fn delete(
    path: web::Path<String>,
    service: web::Data<Arc<ChargerService>>,
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
