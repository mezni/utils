use actix_web::{web, HttpResponse, Result};
use sqlx::postgres::PgPool;
use serde::{Deserialize, Serialize};
use crate::domain::gis::{Station, StationList};
use crate::queries::spatial::SpatialQueryBuilder;
use crate::queries::nearest::NearestQueryService;
use crate::queries::bbox::BoundingBoxQueryService;
use crate::middleware::spatial::RadiusSearchQuery;
use crate::redis::spatial_cache::SpatialCache;

/// Handler for GET /api/v1/driver/nearby
/// Finds charging stations within a radius with Redis caching and clustering support
pub async fn find_nearby(
    pool: web::Data<PgPool>,
    redis_conn: web::Data<ConnectionManager>,
    query: web::Query<NearbySearchQuery>,
) -> Result<HttpResponse> {
    // Validate and parse query parameters
    let radius_query = RadiusSearchQuery {
        latitude: query.lat,
        longitude: query.lon,
        radius_meters: query.radius,
    };

    // Validate radius
    match radius_query.validate() {
        Ok(_) => {},
        Err(e) => {
            return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                "error": "Invalid query parameters",
                "message": e
            })));
        }
    }

    // Use cache-first approach
    let mut cache = SpatialCache::new(redis_conn.into_inner());
    let pool_ref = pool.into_inner();

    match cache.get_cached(query.lat, query.lon, query.radius).await {
        Some(cached_stations) => {
            // Cache hit
            let pagination = crate::domain::gis::Pagination {
                page: query.page.unwrap_or(1),
                limit: query.limit.unwrap_or(20),
                total: cached_stations.len() as u64,
                total_pages: if query.limit.unwrap_or(20) > 0 {
                    (cached_stations.len() as u64 / (query.limit.unwrap_or(20) as u64)).max(1)
                } else {
                    0
                },
            };

            let station_list = StationList {
                data: cached_stations,
                pagination,
            };

            Ok(HttpResponse::Ok().json(station_list))
        }
        None => {
            // Cache miss - execute PostGIS query
            let nearest_service = NearestQueryService::new(pool_ref.clone());

            match nearest_service.find_nearest(query.lat, query.lon, query.limit.unwrap_or(20)).await {
                Ok(stations) => {
                    // Store results in cache
                    if !stations.is_empty() {
                        let cache_entries: Vec<_> = stations
                            .iter()
                            .map(|station| {
                                crate::redis::spatial_cache::create_cache_entry(station)
                            })
                            .collect();

                        cache.cache_results(query.lat, query.lon, query.radius, &cache_entries).await;
                    }

                    // Optimize for mobile app - reduce payload size
                    let optimized_stations = self.optimize_for_mobile(&stations);

                    let pagination = crate::domain::gis::Pagination {
                        page: query.page.unwrap_or(1),
                        limit: query.limit.unwrap_or(20),
                        total: stations.len() as u64,
                        total_pages: if query.limit.unwrap_or(20) > 0 {
                            (stations.len() as u64 / (query.limit.unwrap_or(20) as u64)).max(1)
                        } else {
                            0
                        },
                    };

                    let station_list = StationList {
                        data: optimized_stations,
                        pagination,
                    };

                    Ok(HttpResponse::Ok().json(station_list))
                }
                Err(e) => {
                    eprintln!("Error finding nearby stations: {}", e);
                    Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                        "error": "Failed to find nearby stations",
                        "message": e.to_string()
                    })))
                }
            }
        }
    }
}

/// Optimize stations for mobile app rendering
fn optimize_for_mobile(stations: &[Station]) -> Vec<Station> {
    // Limit to essential fields for mobile
    stations
        .iter()
        .filter_map(|station| {
            Some(Station {
                id: station.id.clone(),
                name: station.name.clone(),
                latitude: station.latitude,
                longitude: station.longitude,
                distance: station.distance,
                amenity: station.amenity.clone(),
                power: None, // Skip power for mobile to reduce payload
                connector_types: station.connector_types.clone(), // Include connector types for filtering
                is_available: station.is_available,
                last_updated: None,
                created_at: None,
            })
        })
        .collect()
}

/// Query parameters for nearby search
#[derive(Debug, Deserialize)]
pub struct NearbySearchQuery {
    /// Latitude
    pub lat: f64,

    /// Longitude
    pub lon: f64,

    /// Radius in meters (min: 100, max: 100000)
    pub radius: i32,

    /// Maximum number of results (optional, default: 20, max: 100)
    #[serde(default = "default_limit", deserialize_with = "validate_limit")]
    pub limit: Option<u32>,

    /// Page number (optional, default: 1)
    #[serde(default = "default_page")]
    pub page: u32,
}

fn default_page() -> u32 {
    1
}

fn default_limit() -> u32 {
    20
}

fn validate_limit(input: &str) -> Result<Option<u32>, serde::de::Error> {
    let limit: u32 = input.parse().map_err(serde::de::Error::custom)?;

    if limit > 100 {
        Err(serde::de::Error::custom("Limit must be at most 100"))
    } else {
        Ok(Some(limit))
    }
}

/// Handler for GET /api/v1/driver/stations/count
/// Gets total count of available charging stations
pub async fn get_station_count(pool: web::Data<PgPool>) -> Result<HttpResponse> {
    let count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM gis.osm_charging_stations WHERE is_available = TRUE",
    )
    .fetch_one(pool.get_ref())
    .await
    .map_err(|e| {
        eprintln!("Error counting stations: {}", e);
        e
    })?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "count": count.0,
        "available": count.0 > 0
    })))
}

/// Handler for GET /api/v1/driver/stations/stats
/// Gets station statistics by amenity type
pub async fn get_station_stats(pool: web::Data<PgPool>) -> Result<HttpResponse> {
    let stats: Vec<StationStats> = sqlx::query_as(
        "SELECT amenity, COUNT(*) as count, MIN(latitude) as min_lat, MAX(latitude) as max_lat, MIN(longitude) as min_lon, MAX(longitude) as max_lon FROM gis.osm_charging_stations GROUP BY amenity ORDER BY count DESC"
    )
    .fetch_all(pool.get_ref())
    .await
    .map_err(|e| {
        eprintln!("Error fetching station stats: {}", e);
        e
    })?;

    Ok(HttpResponse::Ok().json(stats))
}

/// Station statistics by amenity type
#[derive(Debug, Serialize, Deserialize)]
pub struct StationStats {
    pub amenity: String,
    pub count: i64,
    pub min_lat: f64,
    pub max_lat: f64,
    pub min_lon: f64,
    pub max_lon: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_page() {
        assert_eq!(default_page(), 1);
    }

    #[test]
    fn test_default_limit() {
        assert_eq!(default_limit(), 20);
    }

    #[test]
    fn test_validate_limit_valid() {
        assert_eq!(validate_limit("10").unwrap(), Some(10));
        assert_eq!(validate_limit("20").unwrap(), Some(20));
        assert_eq!(validate_limit("100").unwrap(), Some(100));
    }

    #[test]
    fn test_validate_limit_invalid() {
        assert!(validate_limit("0").is_err());
        assert!(validate_limit("101").is_err());
        assert!(validate_limit("-10").is_err());
    }
}
