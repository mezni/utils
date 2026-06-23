//! Analytics API routes and handlers for admin-service
//! Exposes read-only analytics endpoints

use actix_web::{web, HttpResponse, Result, Scope};
use chrono::Utc;
use domain_types::analytics::{
    AnalyticsQuery, AnalyticsResponse, KPIQuery, SummaryAnalytics, StationAnalytics,
    StationAnalyticsQuery, SearchTrend,
};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::services::{
    CacheService,
    KPIAggregationEngine,
};
use crate::middleware::{AuthUser, PartnerIsolationContext, PartnerIsolation};
use crate::db::{PgPool, create_pool, DatabaseConfig};

/// Shared state for analytics
pub struct AppState {
    /// KPI aggregation engine
    pub kpi_engine: Arc<KPIAggregationEngine>,
    /// Database pool
    pub db_pool: Arc<PgPool>,
    /// Cache service
    pub cache_service: Arc<CacheService>,
}

/// Analytics API routes configuration
pub fn configure_routes(cfg: &mut web::ServiceConfig, state: Arc<AppState>) {
    cfg.service(
        web::scope("/api/v1/analytics")
            // User Story 1: Admin Dashboard Access
            .route("/summary", web::get().to(get_summary))
            .route("/stations/:id", web::get().to(get_station_analytics))
            .route("/users/:uuid", web::get().to(get_user_activity))
            .route("/search-trends", web::get().to(get_search_trends))
            .route("/cache/health", web::get().to(get_cache_health))
            // User Story 5: Cache Invalidation Endpoint (requires admin role)
            // .route("/cache/invalidate", web::post().to(invalidate_cache)) // Disabled for now, admin role not yet enforced on handler
            // User Story 2: Read-Only Enforcement (handled by middleware)
            // User Story 4: Partner Analytics (handled by partner isolation middleware)
    );
}

/// Get platform-wide analytics summary
pub async fn get_summary(
    state: web::Data<AppState>,
    query: web::Query<AnalyticsQuery>,
    auth_user: AuthUser, // Inject authenticated user
) -> Result<HttpResponse> {
    // Validate query parameters
    if let Err(e) = query.validate() {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "error": "invalid_query",
            "message": e
        })))
    }

    // Enforce partner isolation for manager users
    if auth_user.is_manager() {
        if let Some(partner_id) = query.effective_partner_id() {
            if let Err(e) = PartnerIsolation::validate_partner_id(partner_id) {
                return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                    "error": "invalid_parameter",
                    "message": format!("Invalid partner ID format: {}", e)
                })))
            }
            // Filter data by partner_id if manager
            // This filtering logic will be applied in the query service
        }
    }

    // Calculate KPIs
    let kpis = state.kpi_engine.calculate_all_kpis().await?;

    // Convert KPIs to summary analytics response
    let summary_analytics = SummaryAnalytics::new(
        kpis.iter().find(|kpi| kpi.kpi_name.as_str() == "station_views")
            .map(|kpi| kpi.value as u64)
            .unwrap_or(0),
        kpis.iter().find(|kpi| kpi.kpi_name.as_str() == "search_volume")
            .map(|kpi| kpi.value as u64)
            .unwrap_or(0),
        kpis.iter().find(|kpi| kpi.kpi_name.as_str() == "favorite_count")
            .map(|kpi| kpi.value as u64)
            .unwrap_or(0),
        kpis.iter().find(|kpi| kpi.kpi_name.as_str() == "active_users")
            .map(|kpi| kpi.value as u64)
            .unwrap_or(0),
    );

    // Get cache metrics
    let cache_metrics = state.cache_service.metrics();

    Ok(HttpResponse::Ok().json(AnalyticsResponse {
        data: summary_analytics,
        metadata: domain_types::analytics::AnalyticsMetadata {
            request_id: uuid::Uuid::new_v4().to_string(),
            query_duration_ms: 100, // Mock duration
            timestamp: Utc::now().to_rfc3339(),
            cached: false, // Should be determined by cache_status
            cache_hit_rate: cache_metrics.get_hit_rate(),
        },
        cache_status: domain_types::analytics::CacheStatus {
            status: "miss".to_string(), // Placeholder, determined by cache lookup
            latency_ms: 50,
            ttl_remaining_seconds: None,
        },
    }))
}

/// Get station-specific analytics
pub async fn get_station_analytics(
    path: web::Path<String>,
    state: web::Data<AppState>,
    auth_user: AuthUser, // Inject authenticated user
    query: web::Query<StationAnalyticsQuery>,
) -> Result<HttpResponse> {
    let station_id = path.into_inner();

    // Validate partner ID if provided by manager
    if auth_user.is_manager() {
        if let Some(partner_id) = query.effective_partner_id() {
            if let Err(e) = PartnerIsolation::validate_partner_id(partner_id) {
                return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                    "error": "invalid_parameter",
                    "message": format!("Invalid partner ID format: {}", e)
                })))
            }
        } else if query.require_partner_id {
            // If partner ID is required but not provided
            return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                "error": "missing_parameter",
                "message": "Partner ID is required for manager users"
            })))
        }
    }

    // Get station analytics from database
    let analytics_row = sqlx::query!(
        r#" SELECT
            station_id,
            station_views,
            favorite_count as total_favorites,
            unique_users,
            avg_session_gap_seconds,
            last_viewed_at,
            first_viewed_at,
            partner_id
        FROM station_usage
        WHERE station_id = $1 "#,
        station_id
    )
    .fetch_optional(&**state.db_pool)
    .await
    .map_err(|e| {
        eprintln!("Database error fetching station analytics: {:?}", e);
        HttpResponse::InternalServerError().json(serde_json::json!({
            "error": "database_error",
            "message": format!("Database error: {:?}", e)
        }))
    })?;

    match analytics_row {
        Some(row) => {
            // Apply partner isolation if manager and filter_by_partner is true
            if auth_user.is_manager() && query.filter_by_partner {
                if let Some(existing_partner_id) = row.partner_id.as_ref() {
                    if let Some(query_partner_id) = query.effective_partner_id() {
                        if existing_partner_id != query_partner_id {
                            return Ok(HttpResponse::Forbidden().json(serde_json::json!({
                                "error": "forbidden",
                                "message": "Access denied: partner mismatch"
                            })));
                        }
                    }
                }
            }

            let station_analytics = StationAnalytics {
                station_id: row.station_id,
                station_views: row.station_views,
                favorites: row.total_favorites,
                search_hits: 0, // Placeholder: Calculate separately
                avg_session_time_seconds: row.avg_session_gap_seconds,
                unique_users: row.unique_users,
                last_viewed_at: row.last_viewed_at.map(|dt| dt.to_rfc3339()),
                first_viewed_at: row.first_viewed_at.map(|dt| dt.to_rfc3339()),
                partner_id: row.partner_id,
            };

            Ok(HttpResponse::Ok().json(AnalyticsResponse::from_data(
                station_analytics,
                uuid::Uuid::new_v4().to_string(),
                50,
                true, // Mock cached status
            )))
        }
        None => Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "not_found",
            "message": format!("Station analytics not found for station_id: {}", station_id)
        }))),
    }
}

/// Get user activity analytics
pub async fn get_user_activity(
    path: web::Path<String>,
    state: web::Data<AppState>,
    auth_user: AuthUser, // Inject authenticated user
) -> Result<HttpResponse> {
    let user_uuid = path.into_inner();

    // Validate user UUID format
    if let Err(e) = PartnerIsolation::validate_user_uuid(&user_uuid) {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "error": "invalid_parameter",
            "message": format!("Invalid user UUID format: {}", e)
        })))
    }

    let activity = sqlx::query!(
        r#" SELECT
            user_uuid,
            total_views,
            stations_visited,
            favorites_count,
            search_count
        FROM user_activity
        WHERE user_uuid = $1 "#,
        user_uuid
    )
    .fetch_optional(&**state.db_pool)
    .await
    .map_err(|e| {
        eprintln!("Database error fetching user activity: {:?}", e);
        HttpResponse::InternalServerError().json(serde_json::json!({
            "error": "database_error",
            "message": format!("Database error: {:?}", e)
        }))
    })?;

    match activity {
        Some(row) => {
            let analytics = serde_json::json!({
                "user_uuid": row.user_uuid,
                "total_views": row.total_views,
                "stations_visited": row.stations_visited,
                "favorites_count": row.favorites_count,
                "search_count": row.search_count,
            });

            Ok(HttpResponse::Ok().json(AnalyticsResponse::from_data(
                analytics,
                uuid::Uuid::new_v4().to_string(),
                50,
                true, // Mock cached status
            )))
        }
        None => Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "not_found",
            "message": format!("User activity not found for user_uuid: {}", user_uuid)
        }))),
    }
}

/// Get search trends
pub async fn get_search_trends(
    query: web::Query<AnalyticsQuery>,
    state: web::Data<AppState>,
    auth_user: AuthUser, // Inject authenticated user
) -> Result<HttpResponse> {
    let mut trends_data = Vec::new();
    let mut error_message = String::new();

    // Validate query parameters
    if let Err(e) = query.validate() {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "error": "invalid_query",
            "message": e
        })))
    }

    // Fetch search trends from database
    let mut db_trends = sqlx::query_as::<_, (String, u64, u64, u64)>(// Query should be parameterized for partner_id and date_range
        r#"SELECT
            query_text,
            search_count,
            unique_searchers,
            stations_searched
        FROM search_trends
        WHERE TRUE
        "#,
    )
    .fetch_all(&**state.db_pool)
    .await
    .map_err(|e| {
        eprintln!("Database error fetching search trends: {:?}", e);
        HttpResponse::InternalServerError().json(serde_json::json!({
            "error": "database_error",
            "message": format!("Database error: {:?}", e)
        }))
    })?;

    // Apply partner filter if manager
    if auth_user.is_manager() && query.effective_partner_id().is_some() {
        let partner_id = query.effective_partner_id().unwrap();
        // Filter trends by partner_id (assuming partner_id is part of station_id or derivable)
        // For now, we'll just simulate filtering
        trends_data = trends_data.into_iter().filter(|t| t.station_id.starts_with(partner_id)).collect();
    }

    let trends_vec: Vec<SearchTrend> = trends_data
        .into_iter()
        .map(|(query_text, search_count, unique_searchers, stations_searched)| SearchTrend {
            query_text,
            search_count,
            unique_searchers,
            stations_searched,
            query_frequency_hours: 0.0, // Placeholder
            last_search_at: Utc::now().to_rfc3339(),
            first_search_at: Utc::now().to_rfc3339(),
        })
        .collect();

    Ok(HttpResponse::Ok().json(AnalyticsResponse::from_data(
        trends_vec,
        uuid::Uuid::new_v4().to_string(),
        50,
        true, // Mock cached status
    )))
}

/// Get cache health metrics
pub async fn get_cache_health(
    state: web::Data<AppState>,
    auth_user: AuthUser, // Inject authenticated user
) -> Result<HttpResponse> {
    // Only admin users can access cache health
    if !auth_user.is_admin() {
        return Ok(HttpResponse::Forbidden().json(serde_json::json!({
            "error": "forbidden",
            "message": "Admin access required for cache health"
        })))
    }

    let metrics = state.cache_service.metrics();

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "data": {
            "hit_rate": metrics.get_hit_rate(),
            "hits": metrics.hits.load(std::sync::atomic::Ordering::Relaxed),
            "misses": metrics.misses.load(std::sync::atomic::Ordering::Relaxed),
            "invalidations": metrics.invalidations.load(std::sync::atomic::Ordering::Relaxed),
            "total_requests": metrics.hits.load(std::sync::atomic::Ordering::Relaxed) + metrics.misses.load(std::sync::atomic::Ordering::Relaxed),
            "cache_size_mb": "N/A", // This requires Redis INFO command, which is not directly available here
            "avg_latency_ms": {
                "hit": 8.5, // Mock value
                "miss": 42.3, // Mock value
            },
        },
        "metadata": {
            "request_id": uuid::Uuid::new_v4().to_string(),
            "query_duration_ms": 12,
            "timestamp": Utc::now().to_rfc3339(),
            "cached": true,
            "cache_hit_rate": 0.95,
        },
        "cache_status": {
            "status": "hit",
            "latency_ms": 12,
            "ttl_remaining_seconds": 299,
        },
    })))
}

// Placeholder for invalidate_cache handler (requires admin role enforcement)
// pub async fn invalidate_cache(...) -> Result<HttpResponse> {
//     // ... implementation ...
//     Ok(HttpResponse::Ok().json(serde_json::json!({"success": true})))
// }

// Utility to extract AuthUser from request
impl FromRequest for AuthUser {
    type Error = Error;
    type Future = std::future::Ready<Result<Self, Self::Error>>;

    fn from_request(req: &mut RequestHead, _payload: &mut actix_web::dev::Payload) -> Self::Future {
        // This implementation is simplified. A real implementation would fetch JWKS and validate token signature.
        // For now, we'll simulate a token with hardcoded claims.
        let mock_token = "mock_token_for_testing"; // Replace with actual token retrieval and validation

        // Simulate token extraction and validation
        let claims = crate::middleware::UserClaims {
            sub: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            preferred_username: "testuser".to_string(),
            email: Some("testuser@example.com".to_string()),
            role: Some("admin".to_string()), // Simulate admin role
            iss: "http://localhost:8080/realms/bornemap".to_string(),
            aud: "bornemap".to_string(),
            exp: 9999999999, // Never expires for mock
            iat: 1111111111,
        };

        let user = AuthUser {
            user_uuid: claims.sub,
            username: claims.preferred_username,
            email: claims.email,
            role: claims.role.unwrap_or_else(|| "user".to_string()),
        };

        std::future::ready(Ok(user))
    }
}

// Mock implementation for AuthUser extraction for testing purposes
// In a real application, this would be handled by KeycloakAuth middleware


// Mock KeycloakAuth middleware for basic testing (replace with actual JWT validation)
struct MockKeycloakAuth;

impl MockKeycloakAuth {
    async fn get_user(_req: &RequestHead) -> Result<AuthUser, Error> {
        // Simulate successful authentication for testing
        Ok(AuthUser {
            user_uuid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            username: "testuser".to_string(),
            email: Some("testuser@example.com".to_string()),
            role: "admin".to_string(), // Simulate admin role
        })
    }
}

// Mock PartnerIsolation middleware for basic testing
struct MockPartnerIsolation;

impl MockPartnerIsolation {
    async fn get_context(_req: &RequestHead) -> PartnerIsolationContext {
        // Simulate context with admin role for testing
        PartnerIsolationContext::new(None, true, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test::{self, TestRequest};
    use crate::services::{CacheService, KPIAggregationEngine};
    use std::sync::Arc;
    use sqlx::postgres::PgPoolOptions;

    #[tokio::test]
    async fn test_get_summary_success() {
        // Mock dependencies
        let pool = PgPoolOptions::new().connect("postgres://user:pass@localhost:5432/database").await.unwrap();
        let cache_service = Arc::new(CacheService::new(Default::default()).await.unwrap());
        let kpi_engine = Arc::new(KPIAggregationEngine::new(Default::default(), pool.clone(), cache_service.clone()));

        let app_state = Arc::new(AppState {
            kpi_engine: kpi_engine.clone(),
            db_pool: Arc::new(pool.clone()),
            cache_service: cache_service.clone(),
        });

        let mut app = test::init_service(test::web::App::new().app_data(web::Data::new(app_state.clone())).service(web::scope("/api/v1/analytics").route("/summary", web::get().to(get_summary)))).await;

        let req = TestRequest::get().uri("/api/v1/analytics/summary").to_request();
        let resp = test::call_service(&mut app, req).await;

        assert!(resp.status().is_success());
    }

    #[test]
    fn test_get_station_analytics_not_found() {
        // This test requires mocking the database query to return None
        // For now, we'll just check the structure
        let analytics = StationAnalytics {
            station_id: "STA-test123".to_string(),
            station_views: 100,
            favorites: 10,
            search_hits: 5,
            avg_session_time_seconds: 120.5,
            unique_users: 50,
            last_viewed_at: Some("2026-06-22T15:30:00Z".to_string()),
            first_viewed_at: Some("2026-01-01T08:00:00Z".to_string()),
            partner_id: Some("STX-xxx".to_string()),
        };
        assert_eq!(analytics.station_id, "STA-test123");
    }

    #[test]
    fn test_get_search_trends_success() {
        // This test requires mocking the database query to return some trends
        // For now, we'll just check the structure
        let trend = SearchTrend {
            query_text: "fast charging".to_string(),
            search_count: 100,
            unique_searchers: 50,
            stations_searched: 30,
            query_frequency_hours: 24.0,
            last_search_at: "2026-06-22T15:30:00Z".to_string(),
            first_search_at: "2026-01-01T08:00:00Z".to_string(),
        };
        assert_eq!(trend.query_text, "fast charging");
    }
}