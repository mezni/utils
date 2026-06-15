use actix_web::{web, HttpResponse, Responder};
use sqlx::PgPool;
use services_shared::domain::NearbyStationRow;
use geo_core;

use crate::models::{
    ErrorResponse, HealthResponse, NearbyQuery, NearbyStationsResponse, StationResponse,
};

pub async fn health() -> impl Responder {
    HttpResponse::Ok().json(HealthResponse {
        status: "ok".to_string(),
    })
}

pub async fn get_nearby_stations(
    pool: web::Data<PgPool>,
    query: web::Query<NearbyQuery>,
) -> impl Responder {
    if !geo_core::is_within_tunisia(query.longitude, query.latitude) {
        return HttpResponse::BadRequest().json(ErrorResponse {
            error: "Coordinates outside Tunisia operational bounds".to_string(),
        });
    }

    let radius = query.radius.unwrap_or(5000.0);
    if radius <= 0.0 {
        return HttpResponse::BadRequest().json(ErrorResponse {
            error: "search_radius_meters must be positive".to_string(),
        });
    }

    let result = sqlx::query_as::<_, NearbyStationRow>(
        "SELECT * FROM gis.get_nearby_stations($1, $2, $3)",
    )
    .bind(query.longitude)
    .bind(query.latitude)
    .bind(radius)
    .fetch_all(pool.get_ref())
    .await;

    match result {
        Ok(rows) => {
            let stations: Vec<StationResponse> = rows
                .into_iter()
                .map(|row| StationResponse {
                    station_id: row.station_id,
                    station_name: row.station_name,
                    station_address: row.station_address,
                    distance_meters: row.distance_meters,
                    latitude: row.latitude,
                    longitude: row.longitude,
                    available_chargers: row.available_chargers.0,
                })
                .collect();

            HttpResponse::Ok().json(NearbyStationsResponse { stations })
        }
        Err(e) => {
            tracing::error!("Database query failed: {:?}", e);
            HttpResponse::InternalServerError().json(ErrorResponse {
                error: "Database query failed".to_string(),
            })
        }
    }
}
