//! Analytics response DTOs for admin-service
//! Common response structure for all analytics endpoints

use serde::{Deserialize, Serialize};
use chrono::DateTime;
use chrono::Utc;

/// Analytics response wrapper with data, metadata, and cache status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyticsResponse<T> {
    /// The analytics data
    pub data: T,
    /// Request metadata
    pub metadata: AnalyticsMetadata,
    /// Cache status
    pub cache_status: CacheStatus,
}

/// Analytics metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyticsMetadata {
    /// Unique request ID
    pub request_id: String,
    /// Query duration in milliseconds
    pub query_duration_ms: u64,
    /// Response timestamp
    pub timestamp: String,
    /// Whether response was cached
    pub cached: bool,
    /// Cache hit rate (0.0 to 1.0)
    pub cache_hit_rate: f64,
}

/// Cache status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStatus {
    /// Status: "hit", "miss", or "error"
    pub status: String,
    /// Cache latency in milliseconds
    pub latency_ms: u64,
    /// TTL remaining in seconds (None if not applicable)
    pub ttl_remaining_seconds: Option<u64>,
}

impl AnalyticsResponse<()> {
    /// Create a new analytics response
    pub fn new(data: (), metadata: AnalyticsMetadata, cache_status: CacheStatus) -> Self {
        Self {
            data,
            metadata,
            cache_status,
        }
    }
}

impl<T> AnalyticsResponse<T>
where
    T: Serialize + Deserialize<'static>,
{
    /// Create response from a data value
    pub fn from_data(data: T, request_id: String, query_duration_ms: u64, cached: bool) -> Self {
        let cache_hit_rate = 0.0; // Can be updated if caching is implemented

        Self {
            data,
            metadata: AnalyticsMetadata {
                request_id,
                query_duration_ms,
                timestamp: Utc::now().to_rfc3339(),
                cached,
                cache_hit_rate,
            },
            cache_status: CacheStatus {
                status: if cached { "hit".to_string() } else { "miss".to_string() },
                latency_ms: 0,
                ttl_remaining_seconds: None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_analytics_response_creation() {
        let response = AnalyticsResponse::new(
            (),
            AnalyticsMetadata {
                request_id: "test-001".to_string(),
                query_duration_ms: 100,
                timestamp: Utc::now().to_rfc3339(),
                cached: false,
                cache_hit_rate: 0.0,
            },
            CacheStatus {
                status: "miss".to_string(),
                latency_ms: 50,
                ttl_remaining_seconds: None,
            },
        );

        assert_eq!(response.metadata.request_id, "test-001");
        assert_eq!(response.metadata.query_duration_ms, 100);
        assert_eq!(response.cache_status.status, "miss");
    }

    #[test]
    fn test_cache_status_hit() {
        let status = CacheStatus {
            status: "hit".to_string(),
            latency_ms: 8,
            ttl_remaining_seconds: Some(300),
        };

        assert_eq!(status.status, "hit");
        assert_eq!(status.ttl_remaining_seconds, Some(300));
    }
}