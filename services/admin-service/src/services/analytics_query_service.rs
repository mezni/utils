//! KPI aggregation engine for admin-service
//! Provides calculations for station_views, search_volume, favorite_count, active_users

use anyhow::{Context, Result};
use sqlx::postgres::PgPool;
use std::sync::Arc;
use tokio::sync::RwLock;

/// KPI aggregation engine configuration
#[derive(Debug, Clone)]
pub struct KPIAggregationConfig {
    /// Maximum query duration in milliseconds
    pub max_query_duration_ms: u64,
    /// Enable caching for KPI calculations
    pub enable_caching: bool,
}

impl Default for KPIAggregationConfig {
    fn default() -> Self {
        Self {
            max_query_duration_ms: 500,
            enable_caching: true,
        }
    }
}

/// KPI names
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KPIName {
    StationViews,
    SearchVolume,
    FavoriteCount,
    ActiveUsers,
}

impl KPIName {
    pub fn as_str(&self) -> &'static str {
        match self {
            KPIName::StationViews => "station_views",
            KPIName::SearchVolume => "search_volume",
            KPIName::FavoriteCount => "favorite_count",
            KPIName::ActiveUsers => "active_users",
        }
    }
}

/// KPI aggregation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KPIResult {
    pub kpi_name: KPIName,
    pub value: f64,
    pub unit: String,
    pub source_table: String,
    pub calculated_at: chrono::DateTime<chrono::Utc>,
}

/// KPI aggregation engine
pub struct KPIAggregationEngine {
    config: KPIAggregationConfig,
    pool: PgPool,
    cache_service: Arc<CacheService>,
    metrics: Arc<AggregationMetrics>,
}

/// Aggregation metrics
#[derive(Debug, Clone)]
pub struct AggregationMetrics {
    pub total_calculations: u64,
    pub cached_calculations: u64,
    pub uncached_calculations: u64,
}

impl AggregationMetrics {
    pub fn new() -> Self {
        Self {
            total_calculations: 0,
            cached_calculations: 0,
            uncached_calculations: 0,
        }
    }

    pub fn cache_hit_rate(&self) -> f64 {
        if self.total_calculations == 0 {
            0.0
        } else {
            self.cached_calculations as f64 / self.total_calculations as f64
        }
    }

    pub fn increment_cached(&self) {
        self.cached_calculations.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn increment_uncached(&self) {
        self.uncached_calculations.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl KPIAggregationEngine {
    /// Create new KPI aggregation engine
    pub fn new(config: KPIAggregationConfig, pool: PgPool, cache_service: Arc<CacheService>) -> Self {
        Self {
            config,
            pool,
            cache_service,
            metrics: Arc::new(AggregationMetrics::new()),
        }
    }

    /// Calculate station_views KPI
    pub async fn calculate_station_views(&self) -> Result<f64> {
        let start = std::time::Instant::now();
        let cache_key = "kpi:station_views";

        // Check cache if enabled
        if self.config.enable_caching {
            if let Some(cached) = self.cache_service.get::<f64>(cache_key).await? {
                self.metrics.increment_cached();
                let duration_ms = start.elapsed().as_millis() as u64;
                eprintln!(
                    "✅ Cached station_views: {} (duration: {}ms)",
                    cached, duration_ms
                );
                return Ok(cached);
            }
        }

        self.metrics.increment_uncached();

        // Calculate from station_usage materialized view
        let value = sqlx::query_scalar::<_, f64>(
            "SELECT SUM(station_views) as total_views FROM station_usage"
        )
        .fetch_one(&self.pool)
        .await
        .context("Failed to calculate station_views KPI")?;

        let duration_ms = start.elapsed().as_millis() as u64;

        // Cache result if enabled
        if self.config.enable_caching {
            self.cache_service.set(cache_key, &value, Some(600)).await?;
        }

        eprintln!(
            "✅ Calculated station_views: {} (duration: {}ms)",
            value, duration_ms
        );

        Ok(value)
    }

    /// Calculate search_volume KPI
    pub async fn calculate_search_volume(&self) -> Result<f64> {
        let start = std::time::Instant::now();
        let cache_key = "kpi:search_volume";

        if self.config.enable_caching {
            if let Some(cached) = self.cache_service.get::<f64>(cache_key).await? {
                self.metrics.increment_cached();
                let duration_ms = start.elapsed().as_millis() as u64;
                eprintln!("✅ Cached search_volume: {} (duration: {}ms)", cached, duration_ms);
                return Ok(cached);
            }
        }

        self.metrics.increment_uncached();

        let value = sqlx::query_scalar::<_, f64>(
            "SELECT COUNT(*) as total FROM analytics_events WHERE event_type = 'SEARCH'"
        )
        .fetch_one(&self.pool)
        .await
        .context("Failed to calculate search_volume KPI")?;

        let duration_ms = start.elapsed().as_millis() as u64;

        if self.config.enable_caching {
            self.cache_service.set(cache_key, &value, Some(600)).await?;
        }

        eprintln!("✅ Calculated search_volume: {} (duration: {}ms)", value, duration_ms);

        Ok(value)
    }

    /// Calculate favorite_count KPI
    pub async fn calculate_favorite_count(&self) -> Result<f64> {
        let start = std::time::Instant::now();
        let cache_key = "kpi:favorite_count";

        if self.config.enable_caching {
            if let Some(cached) = self.cache_service.get::<f64>(cache_key).await? {
                self.metrics.increment_cached();
                let duration_ms = start.elapsed().as_millis() as u64;
                eprintln!("✅ Cached favorite_count: {} (duration: {}ms)", cached, duration_ms);
                return Ok(cached);
            }
        }

        self.metrics.increment_uncached();

        let value = sqlx::query_scalar::<_, f64>(
            "SELECT SUM(favorite_count) as total FROM station_usage"
        )
        .fetch_one(&self.pool)
        .context("Failed to calculate favorite_count KPI")?;

        let duration_ms = start.elapsed().as_millis() as u64;

        if self.config.enable_caching {
            self.cache_service.set(cache_key, &value, Some(600)).await?;
        }

        eprintln!("✅ Calculated favorite_count: {} (duration: {}ms)", value, duration_ms);

        Ok(value)
    }

    /// Calculate active_users KPI
    pub async fn calculate_active_users(&self) -> Result<f64> {
        let start = std::time::Instant::now();
        let cache_key = "kpi:active_users";

        if self.config.enable_caching {
            if let Some(cached) = self.cache_service.get::<f64>(cache_key).await? {
                self.metrics.increment_cached();
                let duration_ms = start.elapsed().as_millis() as u64;
                eprintln!("✅ Cached active_users: {} (duration: {}ms)", cached, duration_ms);
                return Ok(cached);
            }
        }

        self.metrics.increment_uncached();

        let value = sqlx::query_scalar::<_, f64>(
            "SELECT COUNT(DISTINCT user_uuid) as unique_users FROM analytics_events WHERE event_type IN ('VIEW', 'SEARCH', 'FAVORITE')"
        )
        .fetch_one(&self.pool)
        .context("Failed to calculate active_users KPI")?;

        let duration_ms = start.elapsed().as_millis() as u64;

        if self.config.enable_caching {
            self.cache_service.set(cache_key, &value, Some(600)).await?;
        }

        eprintln!("✅ Calculated active_users: {} (duration: {}ms)", value, duration_ms);

        Ok(value)
    }

    /// Calculate all KPIs at once
    pub async fn calculate_all_kpis(&self) -> Result<Vec<KPIResult>> {
        let mut results = Vec::new();

        results.push(KPIResult {
            kpi_name: KPIName::StationViews,
            value: self.calculate_station_views().await?,
            unit: "count".to_string(),
            source_table: "station_usage".to_string(),
            calculated_at: chrono::Utc::now(),
        });

        results.push(KPIResult {
            kpi_name: KPIName::SearchVolume,
            value: self.calculate_search_volume().await?,
            unit: "count".to_string(),
            source_table: "analytics_events".to_string(),
            calculated_at: chrono::Utc::now(),
        });

        results.push(KPIResult {
            kpi_name: KPIName::FavoriteCount,
            value: self.calculate_favorite_count().await?,
            unit: "count".to_string(),
            source_table: "station_usage".to_string(),
            calculated_at: chrono::Utc::now(),
        });

        results.push(KPIResult {
            kpi_name: KPIName::ActiveUsers,
            value: self.calculate_active_users().await?,
            unit: "count".to_string(),
            source_table: "analytics_events".to_string(),
            calculated_at: chrono::Utc::now(),
        });

        Ok(results)
    }

    /// Get aggregation metrics
    pub fn metrics(&self) -> AggregationMetrics {
        self.metrics.clone()
    }

    /// Get cache hit rate
    pub fn cache_hit_rate(&self) -> f64 {
        self.metrics.cache_hit_rate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kpi_name_display() {
        assert_eq!(KPIName::StationViews.as_str(), "station_views");
        assert_eq!(KPIName::SearchVolume.as_str(), "search_volume");
        assert_eq!(KPIName::FavoriteCount.as_str(), "favorite_count");
        assert_eq!(KPIName::ActiveUsers.as_str(), "active_users");
    }

    #[test]
    fn test_kpi_name_equality() {
        assert_eq!(KPIName::StationViews, KPIName::StationViews);
        assert_ne!(KPIName::StationViews, KPIName::SearchVolume);
    }
}