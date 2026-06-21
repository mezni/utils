use actix_web::{get, web, HttpResponse, Responder};
use serde::Deserialize;

use crate::application::nearby_service::{NearbyService, NearbyServiceError};
use crate::infrastructure::NearbyRepository;

#[derive(Deserialize)]
pub struct NearbyParams {
    lat: f64,
    lng: f64,
    radius: f64,
}

#[get("/api/v1/driver/nearby")]
pub async fn nearby_stations(
    params: web::Query<NearbyParams>,
    pool: web::Data<sqlx::PgPool>,
) -> impl Responder {
    let repo = NearbyRepository::new(pool.get_ref().clone());
    let service = NearbyService::new(repo);

    match service.find_nearby(params.lat, params.lng, params.radius).await {
        Ok(stations) => HttpResponse::Ok().json(stations),
        Err(NearbyServiceError::Validation(e)) => {
            HttpResponse::BadRequest().json(serde_json::json!({
                "error": e.to_string()
            }))
        }
        Err(NearbyServiceError::Database(e)) => {
            tracing::error!("db error: {e}");
            HttpResponse::ServiceUnavailable().json(serde_json::json!({
                "error": "database unavailable"
            }))
        }
    }
}
