//! Analytics database writer for telemetry ingestion

use crate::domain_types::events::{LocationMetadata, RoleMetadata, SessionMetadata, SystemMetadata, TelemetryEvent, TelemetryStatus};
use chrono::{DateTime, Utc};
use sqlx::postgres::PgPool;
use tracing::{debug, error, info, warn};

/// Analytics query result
#[derive(Debug, Clone)]
pub struct AnalyticsQuery {
    pub user_id: Option<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub schema_version: Option<String>,
    pub event_type: Option<String>,
    pub page_number: i32,
    pub page_size: i32,
}

/// Analytics query response
#[derive(Debug, Clone)]
pub struct AnalyticsQueryResponse {
    pub events: Vec<TelemetryEvent>,
    pub total_count: i64,
    pub page_number: i32,
    pub page_size: i32,
    pub total_pages: i32,
}

/// Write enriched telemetry event to analytics database
///
/// # Arguments
/// * `pool` - PostgreSQL connection pool
/// * `event` - The enriched telemetry event to persist
///
/// # Returns
/// Result indicating success or failure
pub async fn write_analytics_event(
    pool: &PgPool,
    event: TelemetryEvent,
) -> Result<i64, String> {
    debug!(
        event_id = %event.event_id,
        user_id = %event.user_id,
        event_type = %event.event_type,
        "Writing telemetry event to analytics database"
    );

    let query = r#"
        INSERT INTO analytics_events (
            schema_version,
            event_type,
            event_id,
            user_id,
            timestamp,
            payload,
            idempotency_key,
            location_latitude,
            location_longitude,
            location_country,
            location_city,
            location_source,
            session_start,
            session_duration,
            role,
            service_name,
            event_source,
            status,
            created_at,
            updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
    "#;

    let result = sqlx::query(query)
        .bind(&event.schema_version)
        .bind(format!("{:?}", event.event_type))
        .bind(event.event_id)
        .bind(event.user_id)
        .bind(&event.timestamp)
        .bind(&event.payload)
        .bind(event.idempotency_key)
        .bind(event.enriched_metadata.location.latitude)
        .bind(event.enriched_metadata.location.longitude)
        .bind(event.enriched_metadata.location.country)
        .bind(event.enriched_metadata.location.city)
        .bind(format!("{:?}", event.enriched_metadata.location.location_source))
        .bind(&event.enriched_metadata.session.session_start)
        .bind(event.enriched_metadata.session.session_duration)
        .bind(&event.enriched_metadata.role.role)
        .bind(&event.enriched_metadata.system.service_name)
        .bind(&event.enriched_metadata.system.event_source)
        .bind(format!("{:?}", event.status))
        .bind(Utc::now())
        .bind(Utc::now())
        .execute(pool)
        .await;

    match result {
        Ok(result) => {
            info!(
                event_id = %event.event_id,
                user_id = %event.user_id,
                inserted_rows = result.rows_affected(),
                "Telemetry event written to analytics database"
            );
            Ok(result.rows_affected())
        }
        Err(e) => {
            error!(
                event_id = %event.event_id,
                error = %e,
                "Failed to write telemetry event to analytics database"
            );
            Err(format!("Database write failed: {}", e))
        }
    }
}

/// Execute analytics query
///
/// # Arguments
/// * `pool` - PostgreSQL connection pool
/// * `query` - The analytics query parameters
///
/// # Returns
/// Analytics query response with paginated results
pub async fn execute_analytics_query(
    pool: &PgPool,
    query: AnalyticsQuery,
) -> Result<AnalyticsQueryResponse, String> {
    // Build WHERE clause based on query parameters
    let mut where_clauses: Vec<String> = vec!["1=1".to_string()];
    let mut params: Vec<sqlx::postgres::PgValueRef> = Vec::new();

    if let Some(user_id) = &query.user_id {
        where_clauses.push(format!("user_id = ${}", params.len() + 1));
        params.push(sqlx::postgres::PgValueRef::from(user_id.as_bytes()));
    }

    if let Some(start_time) = &query.start_time {
        where_clauses.push(format!("timestamp >= ${}", params.len() + 1));
        params.push(sqlx::postgres::PgValueRef::from(start_time.to_rfc3339().as_bytes()));
    }

    if let Some(end_time) = &query.end_time {
        where_clauses.push(format!("timestamp <= ${}", params.len() + 1));
        params.push(sqlx::postgres::PgValueRef::from(end_time.to_rfc3339().as_bytes()));
    }

    if let Some(schema_version) = &query.schema_version {
        where_clauses.push(format!("schema_version = ${}", params.len() + 1));
        params.push(sqlx::postgres::PgValueRef::from(schema_version.as_bytes()));
    }

    if let Some(event_type) = &query.event_type {
        where_clauses.push(format!("event_type = ${}", params.len() + 1));
        params.push(sqlx::postgres::PgValueRef::from(event_type.as_bytes()));
    }

    let where_clause = where_clauses.join(" AND ");

    // Count total events
    let count_query = format!(
        "SELECT COUNT(*) FROM analytics_events WHERE {}",
        where_clause
    );

    let total_count: i64 = sqlx::query_scalar(&count_query)
        .bind_all(params.iter())
        .fetch_one(pool)
        .await?;

    // Build pagination query
    let offset = (query.page_number - 1) * query.page_size;
    let select_query = format!(
        "SELECT * FROM analytics_events WHERE {} ORDER BY timestamp DESC LIMIT $1 OFFSET $2",
        where_clause
    );

    let events_result = sqlx::query_as::<_, TelemetryEvent>(&select_query)
        .bind(query.page_size as i64)
        .bind(offset)
        .fetch_all(pool)
        .await;

    let events = match events_result {
        Ok(events) => events,
        Err(e) => {
            error!(
                query = ?query,
                error = %e,
                "Failed to execute analytics query"
            );
            return Err(format!("Query execution failed: {}", e));
        }
    };

    let total_pages = if query.page_size > 0 {
        ((total_count as f64) / (query.page_size as f64)).ceil() as i32
    } else {
        0
    };

    Ok(AnalyticsQueryResponse {
        events,
        total_count,
        page_number: query.page_number,
        page_size: query.page_size,
        total_pages,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_analytics_query_defaults() {
        let query = AnalyticsQuery {
            user_id: None,
            start_time: None,
            end_time: None,
            schema_version: None,
            event_type: None,
            page_number: 1,
            page_size: 100,
        };

        assert_eq!(query.page_number, 1);
        assert_eq!(query.page_size, 100);
    }

    #[test]
    fn test_analytics_query_with_filters() {
        let query = AnalyticsQuery {
            user_id: Some("123e4567-e89b-12d3-a456-426614174000".to_string()),
            start_time: Some(Utc::now() - Duration::hours(24)),
            end_time: Some(Utc::now()),
            schema_version: Some("1.0.0".to_string()),
            event_type: Some("AUTH_LOGIN".to_string()),
            page_number: 1,
            page_size: 50,
        };

        assert_eq!(query.user_id, Some("123e4567-e89b-12d3-a456-426614174000".to_string()));
        assert!(query.start_time.is_some());
        assert!(query.end_time.is_some());
        assert_eq!(query.schema_version, Some("1.0.0".to_string()));
        assert_eq!(query.event_type, Some("AUTH_LOGIN".to_string()));
        assert_eq!(query.page_number, 1);
        assert_eq!(query.page_size, 50);
    }
}
