use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Spatial cache key pattern
/// Format: geo:radius:{lat}:{lon}:{radius}
const CACHE_KEY_PATTERN: &str = "geo:radius:%f:%f:%d";

/// Cache entry for station data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationCacheEntry {
    pub id: String,
    pub name: String,
    pub latitude: f64,
    pub longitude: f64,
    pub distance: f64,
    pub amenity: String,
    pub power: String,
    pub connector_types: Vec<String>,
    pub is_available: bool,
}

/// Redis spatial cache for driver-service
pub struct SpatialCache {
    conn: ConnectionManager,
    /// Default TTL for cache entries (5 minutes)
    default_ttl: Duration,
    /// Maximum cache size in entries (configurable)
    max_entries: usize,
}

impl SpatialCache {
    /// Create a new spatial cache
    pub fn new(conn: ConnectionManager) -> Self {
        Self {
            conn,
            default_ttl: Duration::from_secs(300), // 5 minutes
            max_entries: 100000, // 100K entries
        }
    }

    /// Create a new spatial cache with custom TTL
    pub fn with_ttl(conn: ConnectionManager, ttl_secs: u64) -> Self {
        Self {
            conn,
            default_ttl: Duration::from_secs(ttl_secs),
            max_entries: 100000,
        }
    }

    /// Generate cache key for a radius search
    pub fn cache_key(&self, lat: f64, lon: f64, radius: i32) -> String {
        format!(CACHE_KEY_PATTERN, lat, lon, radius)
    }

    /// Get cached results for a radius search
    pub async fn get_cached(&mut self, lat: f64, lon: f64, radius: i32) -> Option<Vec<StationCacheEntry>> {
        let key = self.cache_key(lat, lon, radius);

        match self.conn.lrange(&key, 0, -1).await {
            Ok(entries) => {
                // Parse entries back into StationCacheEntry structs
                let parsed: Result<Vec<StationCacheEntry>, _> = entries
                    .iter()
                    .map(|entry| serde_json::from_str(entry))
                    .collect();

                match parsed {
                    Ok(station_entries) => {
                        // Update TTL on cache hit
                        let _ = self.conn.expire(&key, self.default_ttl.as_secs()).await;
                        Some(station_entries)
                    }
                    Err(_) => {
                        // Cache entry is invalid JSON, remove it
                        let _ = self.conn.del(&key).await;
                        None
                    }
                }
            }
            Err(e) => {
                eprintln!("Error getting cached results: {}", e);
                None
            }
        }
    }

    /// Store results in cache for a radius search
    pub async fn cache_results(
        &mut self,
        lat: f64,
        lon: f64,
        radius: i32,
        stations: &[StationCacheEntry],
    ) -> Result<(), redis::RedisError> {
        // Check if we need to evict old entries (basic LRU)
        if let Ok(count) = self.conn.llen(&self.cache_key(lat, lon, radius)).await {
            if count as usize + stations.len() > self.max_entries {
                // Evict older entries
                let _ = self.conn.ltrim(&self.cache_key(lat, lon, radius), stations.len() as i64, -1).await;
            }
        }

        // Convert stations to JSON strings
        let json_entries: Result<Vec<String>, _> = stations
            .iter()
            .map(|entry| serde_json::to_string(entry))
            .collect();

        match json_entries {
            Ok(jsons) => {
                // Add to cache list (oldest at head, newest at tail)
                for json in jsons {
                    self.conn.rpush(&self.cache_key(lat, lon, radius), &json).await?;
                }

                // Set TTL
                self.conn.expire(&self.cache_key(lat, lon, radius), self.default_ttl.as_secs()).await?;

                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Invalidate cache for a specific location (clear all radius queries around it)
    pub async fn invalidate_location(&mut self, lat: f64, lon: f64) -> Result<(), redis::RedisError> {
        // Pattern: geo:radius:{lat}:{lon}:*
        let pattern = format!("geo:radius:{}:*", lat);

        // Use SCAN to find and delete keys matching the pattern
        let mut cursor = 0i64;
        loop {
            let (new_cursor, keys): (i64, Vec<String>) = self.conn.scan(&mut cursor, &pattern).await?;

            if !keys.is_empty() {
                let _ = self.conn.del(&keys).await;
            }

            if cursor == 0 {
                break;
            }
        }

        Ok(())
    }

    /// Invalidate cache for a specific station (when it's updated)
    pub async fn invalidate_station(&mut self, station_id: &str) -> Result<(), redis::RedisError> {
        // Pattern: geo:radius:*:{station_id}
        let pattern = format!("geo:radius:*:{}", station_id);

        let mut cursor = 0i64;
        loop {
            let (new_cursor, keys): (i64, Vec<String>) = self.conn.scan(&mut cursor, &pattern).await?;

            if !keys.is_empty() {
                let _ = self.conn.del(&keys).await;
            }

            if cursor == 0 {
                break;
            }
        }

        Ok(())
    }

    /// Get cache statistics
    pub async fn get_stats(&mut self) -> Result<CacheStats, redis::RedisError> {
        let mut info = CacheStats::default();

        // Get number of cache keys
        info.key_count = self.conn.dbsize().await?;

        // Calculate approximate cache size
        let mut cursor = 0i64;
        loop {
            let (new_cursor, keys): (i64, Vec<String>) = self.conn.scan(&mut cursor, "geo:radius:*").await?;
            info.key_count += keys.len() as i64;
            if cursor == 0 {
                break;
            }
        }

        Ok(info)
    }

    /// Clear all cached data
    pub async fn clear_all(&mut self) -> Result<(), redis::RedisError> {
        let mut cursor = 0i64;
        loop {
            let (new_cursor, keys): (i64, Vec<String>) = self.conn.scan(&mut cursor, "geo:radius:*").await?;

            if !keys.is_empty() {
                let _ = self.conn.del(&keys).await;
            }

            if cursor == 0 {
                break;
            }
        }

        Ok(())
    }
}

/// Cache statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CacheStats {
    pub key_count: i64,
}

/// Create a cache entry from a station
pub fn create_cache_entry(station: &crate::domain::gis::Station) -> StationCacheEntry {
    StationCacheEntry {
        id: station.id.clone(),
        name: station.name.clone(),
        latitude: station.latitude,
        longitude: station.longitude,
        distance: station.distance.unwrap_or(0.0),
        amenity: station.amenity.clone(),
        power: station.power.clone().unwrap_or_default(),
        connector_types: station.connector_types.clone().unwrap_or_default(),
        is_available: station.is_available,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_key_generation() {
        let cache = SpatialCache::new(ConnectionManager::new("redis://127.0.0.1").unwrap());
        let key = cache.cache_key(40.7829, -73.9654, 1000);
        assert!(key.contains("geo:radius"));
        assert!(key.contains("40.7829"));
        assert!(key.contains("-73.9654"));
        assert!(key.contains("1000"));
    }

    #[test]
    fn test_create_cache_entry() {
        let station = crate::domain::gis::Station {
            id: "STA-123456789".to_string(),
            name: "Test Station".to_string(),
            latitude: 40.7829,
            longitude: -73.9654,
            amenity: "charging_station".to_string(),
            power: Some("50kW".to_string()),
            connector_types: Some(vec!["Type 2".to_string()]),
            is_available: true,
            distance: Some(123.5),
        };

        let entry = create_cache_entry(&station);
        assert_eq!(entry.id, "STA-123456789");
        assert_eq!(entry.name, "Test Station");
        assert_eq!(entry.latitude, 40.7829);
        assert_eq!(entry.longitude, -73.9654);
        assert_eq!(entry.amenity, "charging_station");
        assert!(entry.is_available);
    }
}
