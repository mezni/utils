//! KPI aggregation engine for admin-service
//! Calculates key performance indicators from materialized views

use anyhow::{Context, Result};
use sqlx::postgres::PgPool;
use chrono::Utc;

use crate::services::CacheService;
use crate::middleware::AuthUser;

/// KPI result
#[derive(Debug, Clone)]
pub struct KPI {
    pub kpi_name: String,
    pub kpi_type: String,
    pub value: f64,
    pub unit: String,
    pub period_start: String,
    pub period_end: String,
    pub filters: Vec<String>,
}

/// KPI aggregation engine
pub struct KPIAggregationEngine {
    pub config: KPIConfig,
    pub db_pool: PgPool,
    pub cache_service: CacheService,
}

/// KPI configuration
#[derive(Debug, Clone)]
pub struct KPIConfig {
    pub enabled: bool,
    pub refresh_interval_seconds: u64,
}

impl Default for KPIConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            refresh_interval_seconds: 300, // 5 minutes
        }
    }
}

impl KPIAggregationEngine {
    /// Create new KPI aggregation engine
    pub fn new(config: KPIConfig, db_pool: PgPool, cache_service: CacheService) -> Self {
        Self {
            config,
            db_pool,
            cache_service,
        }
    }

    /// Calculate all KPIs
    pub async fn calculate_all_kpis(&self) -> Result<Vec<KPI>> {
        if !self.config.enabled {
            return Ok(Vec::new());
        }

        // Try to get KPIs from cache first
        let cache_key = "admin:kpi:all";
        if let Ok(Some(cached)) = self.cache_service.get::<Vec<KPI>>(cache_key).await {
            return Ok(cached);
        }

        // Calculate KPIs from database
        let mut kpis = Vec::new();

        // KPI 1: Station Views
        if let Ok(kpi) = self.calculate_station_views().await {
            kpis.push(kpi);
        }

        // KPI 2: Search Volume
        if let Ok(kpi) = self.calculate_search_volume().await {
            kpis.push(kpi);
        }

        // KPI 3: Favorite Count
        if let Ok(kpi) = self.calculate_favorite_count().await {
            kpis.push(kpi);
        }

        // KPI 4: Active Users
        if let Ok(kpi) = self.calculate_active_users().await {
            kpis.push(kpi);
        }

        // Store in cache
        let _ = self.cache_service.set(cache_key, &kpis, Some(300)).await;

        Ok(kpis)
    }

    /// Calculate station views KPI
    pub async fn calculate_station_views(&self) -> Result<KPI> {
        let result = sqlx::query_scalar::<_, i64>(
            r#"SELECT SUM(station_views) as total_views FROM station_usage"#,
        )
        .fetch_one(&self.db_pool)
        .await
        .context("Failed to calculate station views KPI")?;

        Ok(KPI {
            kpi_name: "station_views".to_string(),
            kpi_type: "count".to_string(),
            value: result as f64,
            unit: "views".to_string(),
            period_start: Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            period_end: Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            filters: vec![],
        })
    }

    /// Calculate search volume KPI
    pub async fn calculate_search_volume(&self) -> Result<KPI> {
        let result = sqlx::query_scalar::<_, i64>(
            r#"SELECT SUM(search_count) as total_searches FROM search_trends"#,
        )
        .fetch_one(&self.db_pool)
        .await
        .context("Failed to calculate search volume KPI")?;

        Ok(KPI {
            kpi_name: "search_volume".to_string(),
            kpi_type: "count".to_string(),
            value: result as f64,
            unit: "searches".to_string(),
            period_start: Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            period_end: Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            filters: vec![],
        })
    }

    /// Calculate favorite count KPI
    pub async fn calculate_favorite_count(&self) -> Result<KPI> {
        let result = sqlx::query_scalar::<_, i64>(
            r#"SELECT SUM(favorite_count) as total_favorites FROM station_usage"#,
        )
        .fetch_one(&self.db_pool)
        .await
        .context("Failed to calculate favorite count KPI")?;

        Ok(KPI {
            kpi_name: "favorite_count".to_string(),
            kpi_type: "count".to_string(),
            value: result as f64,
            unit: "favorites".to_string(),
            period_start: Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            period_end: Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            filters: vec![],
        })
    }

    /// Calculate active users KPI
    pub async fn calculate_active_users(&self) -> Result<KPI> {
        let result = sqlx::query_scalar::<_, i64>(
            r#"SELECT COUNT(DISTINCT user_uuid) as unique_users FROM user_activity"#,
        )
        .fetch_one(&self.db_pool)
        .await
        .context("Failed to calculate active users KPI")?;

        Ok(KPI {
            kpi_name: "active_users".to_string(),
            kpi_type: "count".to_string(),
            value: result as f64,
            unit: "users".to_string(),
            period_start: Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            period_end: Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            filters: vec![],
        })
    }

    /// Calculate KPI for specific station
    pub async fn calculate_station_kpi(
        &self,
        station_id: &str,
    ) -> Result<Option<KPI>> {
        let result = sqlx::query_scalar::<_, i64>(
            r#"SELECT station_views FROM station_usage WHERE station_id = $1"#,
        )
        .bind(station_id)
        .fetch_optional(&self.db_pool)
        .await
        .context("Failed to calculate station KPI")?;

        match result {
            Some(count) => Ok(Some(KPI {
                kpi_name: format!("station_views_{}", station_id),
                kpi_type: "count".to_string(),
                value: count as f64,
                unit: "views".to_string(),
                period_start: Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                period_end: Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                filters: vec![format!("station_id={}", station_id)],
            })),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kpi_config_default() {
        let config = KPIConfig::default();
        assert!(config.enabled);
        assert_eq!(config.refresh_interval_seconds, 300);
    }

    #[test]
    fn test_kpi_new() {
        let config = KPIConfig::default();
        // This test requires actual database connection
        // For now, we'll just verify the structure
        let kpi = KPI {
            kpi_name: "test_kpi".to_string(),
            kpi_type: "count".to_string(),
            value: 100.0,
            unit: "items".to_string(),
            period_start: "2026-01-01T00:00:00Z".to_string(),
            period_end: "2026-01-02T00:00:00Z".to_string(),
            filters: vec![],
        };
        assert_eq!(kpi.kpi_name, "test_kpi");
        assert_eq!(kpi.value, 100.0);
    }
}
