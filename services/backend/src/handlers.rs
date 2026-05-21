use actix_web::{web, HttpResponse, Result};
use sqlx::PgPool;

use crate::models::{StationQuery, StationResponse, ConnectorSummary, TelemetryBatch};

pub async fn get_stations(
    pool: web::Data<PgPool>,
    query: web::Query<StationQuery>,
) -> Result<HttpResponse> {
    let radius_meters = query.radius * 1000.0;

    let stations = sqlx::query_as::<_, (uuid::Uuid, String, Option<String>, f64, f64, bool)>(
        r#"
        SELECT id, name, address,
               ST_X(geom::geometry) as longitude,
               ST_Y(geom::geometry) as latitude,
               is_active
        FROM stations
        WHERE ST_DWithin(
            geom::geography,
            ST_MakePoint($1, $2)::geography,
            $3
        )
        AND is_active = true
        ORDER BY geom <-> ST_MakePoint($1, $2)
        LIMIT 100
        "#,
    )
    .bind(query.lng)
    .bind(query.lat)
    .bind(radius_meters)
    .fetch_all(pool.get_ref())
    .await
    .map_err(|e| {
        tracing::error!("Database query failed: {}", e);
        actix_web::error::ErrorInternalServerError("Failed to fetch stations")
    })?;

    let responses: Vec<StationResponse> = stations
        .into_iter()
        .map(|(id, name, address, lng, lat, is_active)| StationResponse {
            id,
            name,
            address,
            latitude: lat,
            longitude: lng,
            is_active,
            connectors: vec![],
        })
        .collect();

    Ok(HttpResponse::Ok().json(responses))
}

pub async fn get_config(pool: web::Data<PgPool>) -> Result<HttpResponse> {
    let configs = sqlx::query_as::<_, (uuid::Uuid, String, serde_json::Value, chrono::DateTime<chrono::Utc>)>(
        "SELECT id, key, value, updated_at FROM app_configurations",
    )
    .fetch_all(pool.get_ref())
    .await
    .map_err(|e| {
        tracing::error!("Config query failed: {}", e);
        actix_web::error::ErrorInternalServerError("Failed to fetch config")
    })?;

    let config_map: serde_json::Value = serde_json::Value::Object(
        configs
            .into_iter()
            .map(|(_, key, value, _)| (key, value))
            .collect(),
    );

    Ok(HttpResponse::Ok().json(config_map))
}

pub async fn ingest_telemetry(
    pool: web::Data<PgPool>,
    body: web::Json<TelemetryBatch>,
) -> Result<HttpResponse> {
    let event_count = body.events.len();
    tracing::debug!("Received {} telemetry events", event_count);

    // In production, publish to RabbitMQ here
    // For now, just acknowledge
    Ok(HttpResponse::Accepted().json(serde_json::json!({
        "status": "accepted",
        "events": event_count
    })))
}
