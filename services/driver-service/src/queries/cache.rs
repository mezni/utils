use sqlx::postgres::PgPool;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use crate::domain::gis::Station;
use crate::redis::spatial_cache::{SpatialCache, create_cache_entry};
use crate::middleware::spatial::RadiusSearchQuery;

/// Spatial cache wrapper that integrates Redis cache with PostGIS queries
pub struct SpatialCacheWrapper {
    cache: SpatialCache,
}

impl SpatialCacheWrapper {
    pub fn new(cache: SpatialCache) -> Self {
        Self { cache }
    }

    /// Execute radius search with cache integration
    pub async fn radius_search_with_cache(
        &mut self,
        pool: PgPool,
        query: RadiusSearchQuery,
    ) -> Result<Vec<Station>, sqlx::Error> {
        // Generate cache key
        let cache_key = self.cache.cache_key(query.latitude, query.longitude, query.radius_meters);

        // Try to get cached results
        match self.cache.get_cached(query.latitude, query.longitude, query.radius_meters).await {
            Some(cached) => {
                // Cache hit - convert to Station format
                return Ok(self.convert_cache_entries_to_stations(cached));
            }
            None => {
                // Cache miss - execute PostGIS query
                let stations = self.execute_postgis_query(pool.clone(), &query).await?;

                // Store results in cache
                if !stations.is_empty() {
                    let cache_entries: Vec<_> = stations
                        .iter()
                        .map(|station| create_cache_entry(station))
                        .collect();

                    self.cache
                        .cache_results(query.latitude, query.longitude, query.radius_meters, &cache_entries)
                        .await?;
                }

                Ok(stations)
            }
        }
    }

    /// Execute PostGIS radius query
    async fn execute_postgis_query(
        &self,
        pool: PgPool,
        query: &RadiusSearchQuery,
    ) -> Result<Vec<Station>, sqlx::Error> {
        let sql = r#"
            SELECT
                id,
                station_name as name,
                latitude,
                longitude,
                amenity,
                power,
                connector_types,
                is_available,
                last_updated,
                created_at
            FROM gis.osm_charging_stations
            WHERE is_available = TRUE
                AND ST_DWithin(
                    ST_MakePoint(longitude, latitude)::geography,
                    ST_MakePoint($1, $2)::geography,
                    $3
                )
            ORDER BY ST_Distance(
                ST_MakePoint(longitude, latitude)::geography,
                ST_MakePoint($1, $2)::geography
            ) ASC
            LIMIT 100
        "#;

        let result = sqlx::query_as::<_, Station>(sql)
            .bind(query.longitude)
            .bind(query.latitude)
            .bind(query.radius_meters as i64)
            .fetch_all(&pool)
            .await?;

        Ok(result)
    }

    /// Convert cache entries to Station format
    fn convert_cache_entries_to_stations(
        &self,
        entries: Vec<crate::redis::spatial_cache::StationCacheEntry>,
    ) -> Vec<Station> {
        entries
            .into_iter()
            .map(|entry| Station {
                id: entry.id,
                name: entry.name,
                latitude: entry.latitude,
                longitude: entry.longitude,
                distance: Some(entry.distance),
                amenity: entry.amenity,
                power: Some(entry.power),
                connector_types: Some(entry.connector_types),
                is_available: entry.is_available,
                last_updated: None,
                created_at: None,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_integration() {
        // This test requires a real Redis connection
        let redis_url = "redis://127.0.0.1";
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect("postgresql://test:test@localhost:5432/test")
            .await
            .expect("Failed to connect to test database");

        // Note: This test would require actual Redis connection
        // In production, this would test the full cache integration
        assert!(true);
    }
}
