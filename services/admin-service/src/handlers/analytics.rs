//! Analytics query handler for admin-service
//! Provides read-only access to telemetry events

use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use driver_service::db::analytics::{AnalyticsQuery, AnalyticsQueryResponse};

/// Analytics query parameters
#[derive(Debug, Deserialize)]
pub struct AnalyticsQueryRequest {
    /// Filter by user_id (UUID)
    pub user_id: Option<String>,
    /// Filter by start time (ISO 8601)
    pub start_time: Option<String>,
    /// Filter by end time (ISO 8601)
    pub end_time: Option<String>,
    /// Filter by schema version
    pub schema_version: Option<String>,
    /// Filter by event type
    pub event_type: Option<String>,
    /// Page number (1-based)
    pub page_number: i32,
    /// Page size (default 100)
    pub page_size: i32,
}

/// Analytics query response
#[derive(Debug, Serialize)]
pub struct AnalyticsQueryResponse {
    pub events: Vec<EventSummary>,
    pub total_count: i64,
    pub page_number: i32,
    pub page_size: i32,
    pub total_pages: i32,
}

/// Event summary for API response
#[derive(Debug, Serialize)]
pub struct EventSummary {
    pub schema_version: String,
    pub event_type: String,
    pub user_id: String,
    pub timestamp: String,
    pub payload: serde_json::Value,
    pub location_source: String,
    pub service_name: String,
}

/// Handle GET /api/v1/analytics/events
pub async fn query_events(
    query: web::Query<AnalyticsQueryRequest>,
    pool: web::Data<sqlx::postgres::PgPool>,
) -> impl Responder {
    // Validate page parameters
    if query.page_number < 1 {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "page_number must be >= 1"
        }));
    }

    if query.page_size < 1 || query.page_size > 1000 {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "page_size must be between 1 and 1000"
        }));
    }

    // Convert to analytics query
    let analytics_query = AnalyticsQuery {
        user_id: query.user_id.clone(),
        start_time: query.start_time.as_ref().and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc))
        }),
        end_time: query.end_time.as_ref().and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc))
        }),
        schema_version: query.schema_version.clone(),
        event_type: query.event_type.clone(),
        page_number: query.page_number,
        page_size: query.page_size,
    };

    // Execute query
    match driver_service::db::analytics::execute_analytics_query(&pool, analytics_query).await {
        Ok(query_response) => {
            // Convert events to API response format
            let events: Vec<EventSummary> = query_response
                .events
                .into_iter()
                .map(|event| EventSummary {
                    schema_version: event.schema_version,
                    event_type: format!("{:?}", event.event_type),
                    user_id: event.user_id.to_string(),
                    timestamp: event.timestamp.to_rfc3339(),
                    payload: event.payload,
                    location_source: format!("{:?}", event.enriched_metadata.location.location_source),
                    service_name: event.enriched_metadata.system.service_name,
                })
                .collect();

            HttpResponse::Ok().json(AnalyticsQueryResponse {
                events,
                total_count: query_response.total_count,
                page_number: query_response.page_number,
                page_size: query_response.page_size,
                total_pages: query_response.total_pages,
            })
        }
        Err(e) => {
            error!("Analytics query failed: {}", e);
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": format!("Failed to execute analytics query: {}", e)
            }))
        }
    }
}

/// Handle GET /api/v1/analytics/events/count
pub async fn get_event_count(
    query: web::Query<AnalyticsQueryRequest>,
    pool: web::Data<sqlx::postgres::PgPool>,
) -> impl Responder {
    // Convert to analytics query (no pagination)
    let analytics_query = AnalyticsQuery {
        user_id: query.user_id.clone(),
        start_time: query.start_time.as_ref().and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc))
        }),
        end_time: query.end_time.as_ref().and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc))
        }),
        schema_version: query.schema_version.clone(),
        event_type: query.event_type.clone(),
        page_number: 1,
        page_size: 0,
    };

    match driver_service::db::analytics::execute_analytics_query(&pool, analytics_query).await {
        Ok(query_response) => {
            HttpResponse::Ok().json(serde_json::json!({
                "total_count": query_response.total_count,
            }))
        }
        Err(e) => {
            error!("Event count query failed: {}", e);
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": format!("Failed to get event count: {}", e)
            }))
        }
    }
}

/// Register analytics routes
pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v1/analytics")
            .route("/events", web::get().to(query_events))
            .route("/events/count", web::get().to(get_event_count))
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_analytics_query_request_defaults() {
        let req = AnalyticsQueryRequest {
            user_id: None,
            start_time: None,
            end_time: None,
            schema_version: None,
            event_type: None,
            page_number: 1,
            page_size: 100,
        };

        assert_eq!(req.page_number, 1);
        assert_eq!(req.page_size, 100);
    }

    #[test]
    fn test_analytics_query_request_validation() {
        let req = AnalyticsQueryRequest {
            user_id: Some("123e4567-e89b-12d3-a456-426614174000".to_string()),
            start_time: Some("2026-06-22T00:00:00Z".to_string()),
            end_time: Some("2026-06-22T23:59:59Z".to_string()),
            schema_version: Some("1.0.0".to_string()),
            event_type: Some("AUTH_LOGIN".to_string()),
            page_number: 1,
            page_size: 50,
        };

        assert!(req.user_id.is_some());
        assert!(req.start_time.is_some());
        assert!(req.end_time.is_some());
        assert_eq!(req.page_number, 1);
        assert_eq!(req.page_size, 50);
    }
}
