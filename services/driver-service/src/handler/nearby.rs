use actix_web::{web, HttpResponse};
use serde::Deserialize;
use sqlx::PgPool;

use crate::middleware::validation::{validate_coordinates, validate_max_results, validate_radius_m};
use crate::models::error::Result;
use crate::repository::station_repository::get_nearby;

#[derive(Deserialize)]
pub struct NearbyRequest {
    pub lat: f64,
    pub lon: f64,
    #[serde(default = "default_radius")]
    pub radius_m: i32,
    #[serde(default = "default_max_results")]
    pub max_results: i32,
    #[serde(default = "default_visibility")]
    pub visibility: String,
}

fn default_radius() -> i32 { 5000 }
fn default_max_results() -> i32 { 50 }
fn default_visibility() -> String { "active".to_string() }

pub async fn nearby_handler(
    pool: web::Data<PgPool>,
    query: web::Query<NearbyRequest>,
) -> Result<HttpResponse> {
    let req = query.into_inner();

    if let Some(error) = validate_coordinates(req.lat, req.lon) {
        return Ok(HttpResponse::BadRequest().json(error));
    }

    if let Some(error) = validate_radius_m(Some(req.radius_m)) {
        return Ok(HttpResponse::BadRequest().json(error));
    }

    if let Some(error) = validate_max_results(Some(req.max_results)) {
        return Ok(HttpResponse::BadRequest().json(error));
    }

    let response = get_nearby(
        pool.get_ref(),
        req.lat,
        req.lon,
        req.radius_m as f64,
        req.max_results,
        &req.visibility,
    )
    .await?;

    Ok(HttpResponse::Ok().json(response))
}
