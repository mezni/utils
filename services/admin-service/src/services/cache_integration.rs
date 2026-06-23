//! Cache service integration helpers
//! Provides integration between cache service and analytics endpoints

use anyhow::Result;
use std::sync::Arc;
use domain_types::analytics::AnalyticsResponse;
use sqlx::postgres::PgPool;
use crate::services::CacheService;

/// Get data from cache or database
pub async fn get_from_cache_or_db<T, F>(
    cache_service: &Arc<CacheService>,
    db: &PgPool,
    cache_key: &str,
    query: F,
) -> Result<(Option<T>, bool)>
where
    T: serde::de::DeserializeOwned + std::fmt::Debug,
    F: FnOnce() -> Result<T>,
{
    // Try to get from cache
    if let Ok(Some(cached)) = cache_service.get::<T>(cache_key).await {
        return Ok((Some(cached), true));
    }

    // Cache miss: query database
    let data = query()?;
    let hit = false;

    // Store in cache
    cache_service.set(cache_key, &data, Some(300)).await?;

    Ok((Some(data), hit))
}

/// Format cache status for response
pub fn format_cache_status(hit: bool, latency_ms: u64, ttl_seconds: Option<u64>) -> crate::domain_types::analytics::CacheStatus {
    crate::domain_types::analytics::CacheStatus {
        status: if hit { "hit".to_string() } else { "miss".to_string() },
        latency_ms,
        ttl_remaining_seconds: ttl_seconds,
    }
}

/// Create pagination metadata
pub fn create_pagination_metadata(
    page: usize,
    per_page: usize,
    total_items: u64,
) -> crate::domain_types::analytics::PaginationMetadata {
    crate::domain_types::analytics::PaginationMetadata {
        page,
        per_page,
        total_items,
        total_pages: (total_items as f64 / per_page as f64).ceil() as usize,
        previous_page: if page > 1 { Some(page - 1) } else { None },
        next_page: if page < (total_items as usize / per_page) + 1 { Some(page + 1) } else { None },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pagination_metadata() {
        let meta = create_pagination_metadata(1, 100, 500);

        assert_eq!(meta.page, 1);
        assert_eq!(meta.per_page, 100);
        assert_eq!(meta.total_items, 500);
        assert_eq!(meta.total_pages, 5);
        assert!(meta.previous_page.is_none());
        assert!(meta.next_page.is_some());
    }
}