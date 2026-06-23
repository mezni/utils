//! Redis cache service for admin-service
//! Provides caching layer for analytics queries

use anyhow::{Context, Result};
use redis::aio::MultiplexedConnection;
use redis::aio;
use std::sync::Arc;
use tokio::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;

/// Cache configuration
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Redis connection URL
    pub redis_url: String,
    /// Default TTL for cache entries in seconds
    pub default_ttl_seconds: u64,
    /// Maximum connection pool size
    pub max_connections: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            redis_url: std::env::var("REDIS_URL").unwrap_or_else(|_| {
                "redis://localhost:6379".to_string()
            }),
            default_ttl_seconds: 300, // 5 minutes
            max_connections: 10,
        }
    }
}

/// Cache service for managing analytics query results
pub struct CacheService {
    client: redis::aio::MultiplexedConnection,
    config: CacheConfig,
    metrics: Arc<CacheMetrics>,
}

/// Cache metrics for monitoring
#[derive(Debug, Clone)]
pub struct CacheMetrics {
    pub hits: Arc<AtomicU64>,
    pub misses: Arc<AtomicU64>,
    pub invalidations: Arc<AtomicU64>,
}

impl CacheMetrics {
    pub fn new() -> Self {
        Self {
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
            invalidations: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn get_hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_invalidation(&self) {
        self.invalidations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn reset(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.invalidations.store(0, Ordering::Relaxed);
    }
}

impl CacheService {
    /// Create new cache service with configuration
    pub async fn new(config: CacheConfig) -> Result<Self> {
        // Create Redis connection pool
        let client = redis::Client::open(config.redis_url.clone())
            .context("Failed to connect to Redis")?;

        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .context("Failed to get Redis connection")?;

        // Test connection
        redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .context("Failed to ping Redis")?;

        Ok(Self {
            client: conn,
            config,
            metrics: Arc::new(CacheMetrics::new()),
        })
    }

    /// Get value from cache
    pub async fn get<T: for<'a> serde::Deserialize<'a>>(&self, key: &str) -> Result<Option<T>> {
        let mut conn = self.client.clone();

        match redis::cmd("GET")
            .arg(key)
            .query_async(&mut conn)
            .await
        {
            Ok(Some(value)) => {
                self.metrics.record_hit();
                let deserialized: T = serde_json::from_str(&value)
                    .context("Failed to deserialize cached value")?;
                Ok(Some(deserialized))
            }
            Ok(None) => {
                self.metrics.record_miss();
                Ok(None)
            }
            Err(e) if e.to_string().contains("No such key") => {
                self.metrics.record_miss();
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Set value in cache with TTL
    pub async fn set<T: serde::Serialize>(
        &self,
        key: &str,
        value: &T,
        ttl_seconds: Option<u64>,
    ) -> Result<()> {
        let mut conn = self.client.clone();
        let ttl = ttl_seconds.unwrap_or(self.config.default_ttl_seconds);

        let serialized = serde_json::to_string(value)
            .context("Failed to serialize value for cache")?;

        redis::cmd("SET")
            .arg(key)
            .arg(&serialized)
            .arg("EX")
            .arg(ttl)
            .query_async(&mut conn)
            .await
            .context("Failed to set value in cache")?;

        Ok(())
    }

    /// Delete value from cache
    pub async fn delete(&self, key: &str) -> Result<()> {
        let mut conn = self.client.clone();

        redis::cmd("DEL")
            .arg(key)
            .query_async(&mut conn)
            .await
            .context("Failed to delete value from cache")?;

        self.metrics.record_invalidation();

        Ok(())
    }

    /// Delete multiple keys from cache
    pub async fn delete_multi(&self, keys: &[&str]) -> Result<()> {
        let mut conn = self.client.clone();

        redis::cmd("DEL")
            .arg(keys)
            .query_async(&mut conn)
            .await
            .context("Failed to delete multiple keys from cache")?;

        self.metrics.record_invalidation();

        Ok(())
    }

    /// Get cache metrics
    pub fn metrics(&self) -> Arc<CacheMetrics> {
        self.metrics.clone()
    }

    /// Get hit rate
    pub fn hit_rate(&self) -> f64 {
        self.metrics.get_hit_rate()
    }

    /// Clear all cache (use with caution)
    pub async fn flush_all(&self) -> Result<()> {
        let mut conn = self.client.clone();

        redis::cmd("FLUSHALL")
            .query_async(&mut conn)
            .await
            .context("Failed to flush all cache")?;

        self.metrics.reset();

        Ok(())
    }

    /// Get cache statistics
    pub async fn info(&self) -> Result<CacheInfo> {
        let mut conn = self.client.clone();

        let info: redis::InfoCmd = redis::cmd("INFO")
            .arg("stats")
            .query_async(&mut conn)
            .await
            .context("Failed to get Redis info")?;

        Ok(CacheInfo {
            keyspace_hits: info.get("keyspace_hits").unwrap_or(0).parse::<u64>().unwrap_or(0),
            keyspace_misses: info.get("keyspace_misses").unwrap_or(0).parse::<u64>().unwrap_or(0),
            total_keys: info.get("total_keys").unwrap_or(0).parse::<u64>().unwrap_or(0),
            used_memory_human: info.get("used_memory_human").unwrap_or("0").to_string(),
        })
    }
}

/// Cache information for monitoring
#[derive(Debug, Clone)]
pub struct CacheInfo {
    pub keyspace_hits: u64,
    pub keyspace_misses: u64,
    pub total_keys: u64,
    pub used_memory_human: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_cache_set_and_get() {
        let config = CacheConfig {
            redis_url: "redis://127.0.0.1:6379".to_string(),
            default_ttl_seconds: 60,
            max_connections: 10,
        };

        let cache = CacheService::new(config)
            .await
            .expect("Failed to create cache service");

        let test_value = 42;
        cache.set("test_key", &test_value, Some(10))
            .await
            .expect("Failed to set value");

        let retrieved: i32 = cache.get("test_key").await.expect("Failed to get value");
        assert_eq!(retrieved, 42);

        cache.delete("test_key").await.expect("Failed to delete key");
    }
}