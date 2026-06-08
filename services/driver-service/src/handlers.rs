use actix_web::{web, HttpResponse};
use sqlx::PgPool;
use std::sync::Arc;

use crate::{
    config::PostgresUrl,
    error::AppError,
    models::{NearbyStationsRequest, NearbyStationsResponse, StationResponse},
};

pub async fn health_check_handler(
    pool: web::Data<PgPool>,
) -> Result<HttpResponse, AppError> {
    sqlx::query("SELECT 1")
        .fetch_one(pool.get_ref())
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

pub async fn stations_nearby_handler(
    query: web::Query<NearbyStationsRequest>,
    pool: web::Data<PgPool>,
) -> Result<HttpResponse, AppError> {
    validate_nearby_params(&query.lat, &query.lng, &query.radius_km)?;

    let radius_meters = query.radius_km * 1000.0;

    let stations = sqlx::query_as::<_, StationResponse>(
        r#"
        SELECT
            s.id::text AS id,
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
    )
    .bind(query.lat)
    .bind(query.lng)
    .bind(radius_meters)
    .fetch_all(pool.get_ref())
    .await
    .map_err(|e| {
        tracing::error!("Database query failed for stations nearby: {}", e);
        AppError::DatabaseError(format!("Database error: {}", e))
    })?;

    Ok(HttpResponse::Ok().json(NearbyStationsResponse { stations }))
}

fn validate_nearby_params(
    lat: &f64,
    lng: &f64,
    radius_km: &f64,
) -> Result<(), AppError> {
    if *lat < -90.0 || *lat > 90.0 {
        return Err(AppError::ValidationError(format!(
            "latitude must be between -90 and 90, got: {}",
            lat
        )));
    }

    if *lng < -180.0 || *lng > 180.0 {
        return Err(AppError::ValidationError(format!(
            "longitude must be between -180 and 180, got: {}",
            lng
        )));
    }

    if *radius_km < 0.1 || *radius_km > 100.0 {
        return Err(AppError::ValidationError(format!(
            "radius_km must be between 0.1 and 100, got: {}",
            radius_km
        )));
    }

    Ok(())
}
