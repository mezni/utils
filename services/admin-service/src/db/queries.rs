//! Analytics query functions for admin-service
//! Provides read-only access to telemetry events in analytics_db

use driver_service::db::analytics::AnalyticsQuery;
use serde::Deserialize;

/// Analytics query parameters
#[derive(Debug, Deserialize, Clone)]
pub struct AnalyticsQueryParams {
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
    /// Page number (1-based, default 1)
    pub page_number: i32,
    /// Page size (default 100, max 1000)
    pub page_size: i32,
}

impl Default for AnalyticsQueryParams {
    fn default() -> Self {
        Self {
            user_id: None,
            start_time: None,
            end_time: None,
            schema_version: None,
            event_type: None,
            page_number: 1,
            page_size: 100,
        }
    }
}

/// Get analytics events with filtering and pagination
///
/// # Arguments
/// * `pool` - PostgreSQL connection pool
/// * `params` - Query parameters for filtering and pagination
///
/// # Returns
/// Analytics query response with paginated results
pub async fn get_analytics_events(
    pool: &sqlx::postgres::PgPool,
    params: AnalyticsQueryParams,
) -> Result<driver_service::db::analytics::AnalyticsQueryResponse, String> {
    let analytics_query = AnalyticsQuery {
        user_id: params.user_id,
        start_time: params.start_time.as_ref().and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc))
        }),
        end_time: params.end_time.as_ref().and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc))
        }),
        schema_version: params.schema_version.clone(),
        event_type: params.event_type.clone(),
        page_number: params.page_number,
        page_size: params.page_size,
    };

    driver_service::db::analytics::execute_analytics_query(pool, analytics_query).await
}

/// Get event count with filtering
///
/// # Arguments
/// * `pool` - PostgreSQL connection pool
/// * `params` - Query parameters for filtering
///
/// # Returns
/// Total count of events matching filters
pub async fn get_event_count(
    pool: &sqlx::postgres::PgPool,
    params: AnalyticsQueryParams,
) -> Result<i64, String> {
    let analytics_query = AnalyticsQuery {
        user_id: params.user_id,
        start_time: params.start_time.as_ref().and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc))
        }),
        end_time: params.end_time.as_ref().and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc))
        }),
        schema_version: params.schema_version.clone(),
        event_type: params.event_type.clone(),
        page_number: 1,
        page_size: 0,
    };

    match driver_service::db::analytics::execute_analytics_query(pool, analytics_query).await {
        Ok(response) => Ok(response.total_count),
        Err(e) => Err(e),
    }
}
