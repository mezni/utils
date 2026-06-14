use actix_web::{web, HttpResponse};
use sqlx::PgPool;
use utoipa::path;
use crate::domain::{ProximityQuery, ProximityResponse};
use crate::error::DriverServiceError;
use services_shared::domain::NearbyStationRow;

/// Get nearby charging stations within radius of driver location
///
/// Queries PostGIS spatial index for fast proximity lookups. Returns all
/// available stations with aggregated charger details within the search radius.
#[utoipa::path(
    get,
    path = "/driver/nearby",
    params(
        ("longitude" = f64, Query, description = "Driver longitude coordinate"),
        ("latitude" = f64, Query, description = "Driver latitude coordinate"),
        ("search_radius_meters" = Option<f64>, Query, description = "Search radius in meters, defaults to 5000m")
    ),
    responses(
        (status = 200, description = "Successfully retrieved nearby stations", body = ProximityResponse),
        (status = 400, description = "Invalid coordinates or out of bounds"),
        (status = 500, description = "Internal server error")
    )
)]
#[actix_web::get("/driver/nearby")]
pub async fn get_nearby_stations(
    query: web::Query<ProximityQuery>,
    pool: web::Data<PgPool>,
) -> Result<HttpResponse, DriverServiceError> {
    // Validate coordinate bounds
    if !geo_core::is_within_tunisia(query.longitude, query.latitude) {
        return Err(DriverServiceError::OutOfBounds);
    }

    let search_radius = query.search_radius_meters.unwrap_or(5000.0);

    // Execute spatial proximity query
    let stations: Vec<NearbyStationRow> = sqlx::query_as::<_, NearbyStationRow>(
        "SELECT * FROM gis.get_nearby_stations($1, $2, $3)"
    )
    .bind(query.longitude)
    .bind(query.latitude)
    .bind(search_radius)
    .fetch_all(pool.get_ref())
    .await
    .map_err(|e| {
        tracing::error!("Database error in proximity query: {}", e);
        DriverServiceError::DatabaseError(format!("Failed to query nearby stations: {}", e))
    })?;

    let count = stations.len();

    tracing::debug!(
        "Proximity query completed: {} stations found within {}m",
        count,
        search_radius
    );

    Ok(HttpResponse::Ok().json(ProximityResponse { stations, count }))
}
