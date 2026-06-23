//! Telemetry API handler and routes for driver-service

use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::db::analytics::{write_analytics_event, AnalyticsQuery};
use crate::db::pool::AnalyticsDb;
use crate::middleware::enrichment::enrich_event;
use crate::middleware::idempotency::generate_idempotency_key;
use crate::middleware::telemetry::log_to_dead_letter;
use crate::middleware::telemetry::DeadLetterError;
use crate::middleware::validation::{validate_event, validate_schema_version, validate_event_type, validate_user_id, validate_timestamp, validate_payload, ValidationError};
use domain_types::events::{EnrichedMetadata, EventType, TelemetryEvent, TelemetryStatus};

/// Telemetry event ingestion request
#[derive(Debug, Deserialize)]
pub struct TelemetryIngestionRequest {
    /// Schema version (must be "1.0.0")
    pub schema_version: String,
    /// Event type
    pub event_type: String,
    /// Event ID (UUID)
    pub event_id: String,
    /// User ID (UUID)
    pub user_id: String,
    /// Event timestamp (ISO 8601)
    pub timestamp: String,
    /// Event payload (JSON object)
    pub payload: serde_json::Value,
    /// Location source
    pub location_source: Option<String>,
    /// Additional location data
    pub location: Option<String>,
    /// Request ID for traceability
    pub request_id: Option<String>,
}

/// Telemetry ingestion response
#[derive(Debug, Serialize)]
pub struct TelemetryIngestionResponse {
    pub success: bool,
    pub message: String,
    pub event_id: String,
    pub idempotency_key: String,
}

/// Handle POST /api/v1/telemetry/events
pub async fn ingest_events(
    pool: web::Data<AnalyticsDb>,
    req: web::Json<TelemetryIngestionRequest>,
) -> impl Responder {
    let db = &pool.0;
    info!(
        request_id = ?req.request_id,
        event_type = %req.event_type,
        user_id = %req.user_id,
        "Received telemetry ingestion request"
    );

    // Validate schema version
    if let Err(e) = validate_schema_version(&req.schema_version) {
        error!(
            request_id = ?req.request_id,
            error = %e,
            "Schema version validation failed"
        );
        return HttpResponse::BadRequest().json(serde_json::json!({
            "success": false,
            "message": format!("Schema version validation failed: {}", e),
        }));
    }

    // Validate event type
    let event_type = match validate_event_type(&req.event_type) {
        Ok(et) => et,
        Err(e) => {
            error!(
                request_id = ?req.request_id,
                error = %e,
                "Event type validation failed"
            );
            return HttpResponse::BadRequest().json(serde_json::json!({
                "success": false,
                "message": format!("Event type validation failed: {}", e),
            }));
        }
    };

    // Validate user_id as UUID
    let user_id = match validate_user_id(&req.user_id) {
        Ok(_) => req.user_id.clone(),
        Err(e) => {
            error!(
                request_id = ?req.request_id,
                error = %e,
                "User ID validation failed"
            );
            return HttpResponse::BadRequest().json(serde_json::json!({
                "success": false,
                "message": format!("User ID validation failed: {}", e),
            }));
        }
    };

    // Validate timestamp
    let timestamp = match validate_timestamp(&req.timestamp) {
        Ok(dt) => dt,
        Err(e) => {
            error!(
                request_id = ?req.request_id,
                error = %e,
                "Timestamp validation failed"
            );
            return HttpResponse::BadRequest().json(serde_json::json!({
                "success": false,
                "message": format!("Timestamp validation failed: {}", e),
            }));
        }
    };

    // Validate payload
    if let Err(e) = validate_payload(&req.payload) {
        error!(
            request_id = ?req.request_id,
            error = %e,
            "Payload validation failed"
        );
        return HttpResponse::BadRequest().json(serde_json::json!({
            "success": false,
            "message": format!("Payload validation failed: {}", e),
        }));
    }

    // Generate idempotency key
    let idempotency_key = generate_idempotency_key();

    // Enrich event with location, session, role, and system metadata
    let role = Some("driver".to_string()); // Simplified - in production, extract from JWT
    let (role_metadata, session_metadata, system_metadata) = enrich_event(
        uuid::Uuid::parse_str(&user_id).unwrap(),
        role,
        "driver-service",
        &format!("{:?}", event_type),
    );

    // Enrich location with provenance
    let location_source_str = req.location_source.as_deref().unwrap_or("event_location");
    let enriched_location = crate::middleware::enrichment::enrich_location(req.location, location_source_str);

    // Create enriched metadata
    let enriched_metadata = EnrichedMetadata {
        location: enriched_location,
        session: session_metadata,
        role: role_metadata,
        system: system_metadata,
    };

    // Create telemetry event
    let event = TelemetryEvent {
        schema_version: req.schema_version.clone(),
        event_type,
        event_id: uuid::Uuid::parse_str(&req.event_id).unwrap_or_else(|_| uuid::Uuid::new_v4()),
        user_id: uuid::Uuid::parse_str(&user_id).unwrap_or_else(|_| uuid::Uuid::new_v4()),
        timestamp,
        payload: req.payload.clone(),
        idempotency_key,
        enriched_metadata,
        status: TelemetryStatus::Pending,
    };

    // Validate complete event
    if let Err(e) = validate_event(&event) {
        error!(
            request_id = ?req.request_id,
            error = %e,
            "Event validation failed"
        );

        // Log to dead-letter store
        if let Err(dead_letter_error) = log_to_dead_letter(
            req.event_id.clone(),
            serde_json::to_value(&req.payload).unwrap_or_default(),
            &e,
            req.request_id.clone(),
        ) {
            error!(
                request_id = ?req.request_id,
                dead_letter_error = %dead_letter_error,
                "Failed to log event to dead-letter store"
            );
        }

        return HttpResponse::BadRequest().json(serde_json::json!({
            "success": false,
            "message": format!("Event validation failed: {}", e),
        }));
    }

    // Write event to analytics database
    match write_analytics_event(db, event.clone()).await {
        Ok(rows) => {
            if rows == 0 {
                // Event was rejected (likely duplicate)
                info!(
                    request_id = ?req.request_id,
                    event_id = %req.event_id,
                    "Event rejected - duplicate detected"
                );
                return HttpResponse::Ok().json(serde_json::json!({
                    "success": true,
                    "message": "Event rejected (duplicate detected)",
                    "event_id": req.event_id,
                    "idempotency_key": idempotency_key.to_string(),
                }));
            }

            info!(
                request_id = ?req.request_id,
                event_id = %req.event_id,
                user_id = %user_id,
                rows_affected = rows,
                "Telemetry event successfully ingested"
            );

            HttpResponse::Ok().json(serde_json::json!({
                "success": true,
                "message": "Telemetry event successfully ingested",
                "event_id": req.event_id,
                "idempotency_key": idempotency_key.to_string(),
            }))
        }
        Err(e) => {
            error!(
                request_id = ?req.request_id,
                event_id = %req.event_id,
                error = %e,
                "Failed to ingest telemetry event"
            );
            HttpResponse::InternalServerError().json(serde_json::json!({
                "success": false,
                "message": format!("Failed to ingest event: {}", e),
            }))
        }
    }
}

/// Register telemetry routes
pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v1/telemetry")
            .route("/events", web::post().to(ingest_events))
    );
}
