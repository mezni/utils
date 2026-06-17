use actix_web::{web, HttpResponse};
use serde::Deserialize;
use sqlx::PgPool;

use crate::middleware::validation::{validate_coordinates, validate_max_results, validate_radius_m};
use crate::models::error::{ErrorResponse, ErrorDetail, Result, ResponseMeta};
use crate::repository::station_repository::get_nearby;

#[derive(Deserialize)]
pub struct NearbyRequest {
    pub lat: f64,
    pub lon: f64,
    #[serde(default = "default_radius")]
    pub radius_m: Option<i32>,
    #[serde(default = "default_max_results")]
    pub max_results: Option<i32>,
    #[serde(default = "default_visibility")]
    pub visibility: Option<String>,
}

fn default_radius() -> i32 { 5000 }
fn default_max_results() -> i32 { 50 }
fn default_visibility() -> String { "active".to_string() }

pub async fn nearby_handler(
    pool: web::Data<PgPool>,
    query: web::Query<NearbyRequest>,
) -> Result<HttpResponse> {
    let req = query.into_inner();

    // Validate coordinates
    if let Some(error) = validate_coordinates(
        sqlx::types::BigDecimal::from(req.lat),
        sqlx::types::BigDecimal::from(req.lon),
    ) {
        return Ok(HttpResponse::BadRequest().json(error));
    }

    // Validate radius
    if let Some(error) = validate_radius_m(req.radius_m) {
        return Ok(HttpResponse::BadRequest().json(error));
    }

    // Validate max results
    if let Some(error) = validate_max_results(req.max_results) {
        return Ok(HttpResponse::BadRequest().json(error));
    }

    // Execute spatial query
    let response = get_nearby(
        pool.get_ref(),
        req.lat,
        req.lon,
        req.radius_m.unwrap_or(5000) as f64,
        req.max_results.unwrap_or(50),
        req.visibility.as_deref().unwrap_or("active"),
    )
    .await?;

    Ok(HttpResponse::Ok().json(response))
}
