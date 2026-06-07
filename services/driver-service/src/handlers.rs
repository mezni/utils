// Handlers module
use actix_web::web;
use std::sync::Arc;

use crate::{
    config::PostgresUrl,
    db::create_pool,
    error::AppError,
    models::{NearbyStationsRequest, NearbyStationsResponse, StationResponse},
};

/// Health check handler
/// Returns service status and database connection status
pub async fn health_check_handler(
    postgres_url: web::Data<Arc<PostgresUrl>>,
) -> Result<HttpResponse, AppError> {
    // Try to create a connection pool to verify database connectivity
    let pool = create_pool(postgres_url.as_ref())
        .await
        .map_err(|e| {
            tracing::error!("Database connection failed: {}", e);
            AppError::HealthCheckError(format!("Database error: {}", e))
        })?;

    // Test the connection with a simple query
    sqlx::query("SELECT 1")
        .fetch_one(&pool)
        .await
        .map_err(|e| {
            tracing::error!("Database query failed during health check: {}", e);
            AppError::HealthCheckError(format!("Database error: {}", e))
        })?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "status": "ok",
        "service": "driver-service",
        "db": "ok"
    })))
}

/// Stations nearby handler
/// Queries inventory.station table using spatial query ST_DWithin
/// Returns stations sorted by distance from the requested location
pub async fn stations_nearby_handler(
    query: web::Query<NearbyStationsRequest>,
    pool: web::Data<ev_db::PgPool>,
) -> Result<HttpResponse, AppError> {
    // Validate parameters
    validate_nearby_params(&query.lat, &query.lng, &query.radius_km)?;

    // Calculate spatial query: ST_DWithin using meters (radius * 1000)
    let radius_meters = query.radius_km * 1000.0;

    // Query stations within radius
    let stations = sqlx::query_as!(
        StationResponse,
        r#"
        SELECT
            s.id,
            s.name,
            s.latitude,
            s.longitude,
            ST_Distance(
                ST_SetSRID(ST_MakePoint($1, $2), 4326),
                g.geom
            ) / 1000.0 AS distance_km
        FROM gis.station_locations g
        JOIN inventory.station s ON g.station_id = s.id
        WHERE ST_DWithin(g.geom, ST_SetSRID(ST_MakePoint($1, $2), 4326), $3)
        ORDER BY distance_km ASC
        "#,
        query.lat,
        query.lng,
        radius_meters
    )
    .fetch_all(&pool)
    .await
    .map_err(|e| {
        tracing::error!("Database query failed for stations nearby: {}", e);
        AppError::DatabaseError(format!("Database error: {}", e))
    })?;

    Ok(HttpResponse::Ok().json(NearbyStationsResponse { stations }))
}

/// Validate nearby stations request parameters
fn validate_nearby_params(
    lat: &f64,
    lng: &f64,
    radius_km: &f64,
) -> Result<(), AppError> {
    // Validate latitude: -90 to 90
    if *lat < -90.0 || *lat > 90.0 {
        return Err(AppError::ValidationError(format!(
            "latitude must be between -90 and 90, got: {}",
            lat
        )));
    }

    // Validate longitude: -180 to 180
    if *lng < -180.0 || *lng > 180.0 {
        return Err(AppError::ValidationError(format!(
            "longitude must be between -180 and 180, got: {}",
            lng
        )));
    }

    // Validate radius_km: 0.1 to 100
    if *radius_km < 0.1 || *radius_km > 100.0 {
        return Err(AppError::ValidationError(format!(
            "radius_km must be between 0.1 and 100, got: {}",
            radius_km
        )));
    }

    Ok(())
}
