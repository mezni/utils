use actix_web::{web, HttpResponse};
use serde::Deserialize;
use sqlx::PgPool;
use tracing::info;

use crate::models::NearbyStation;

#[derive(Deserialize)]
pub struct NearbyQuery {
    lat: f64,
    lng: f64,
    radius: f64,
}

pub async fn nearby(
    query: web::Query<NearbyQuery>,
    pool: web::Data<PgPool>,
) -> HttpResponse {
    let lat = query.lat;
    let lng = query.lng;
    let radius = query.radius;

    if !(-90.0..=90.0).contains(&lat) {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Latitude must be between -90 and 90"
        }));
    }
    if !(-180.0..=180.0).contains(&lng) {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Longitude must be between -180 and 180"
        }));
    }
    if !(1.0..=200_000.0).contains(&radius) {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Radius must be between 1 and 200000 meters"
        }));
    }

    let result = sqlx::query_as::<_, NearbyStation>(
        "SELECT station_id, station_name, latitude, longitude, distance_meters, is_private, partner_name FROM gis.get_nearby_stations($1, $2, $3)",
    )
    .bind(lng)
    .bind(lat)
    .bind(radius)
    .fetch_all(pool.get_ref())
    .await;

    match result {
        Ok(stations) => {
            info!(
                lat = lat,
                lng = lng,
                radius = radius,
                count = stations.len(),
                "Nearby stations query completed"
            );
            HttpResponse::Ok().json(serde_json::json!({ "stations": stations }))
        }
        Err(e) => {
            tracing::error!(error = %e, "Database query failed");
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Failed to query nearby stations"
            }))
        }
    }
}
