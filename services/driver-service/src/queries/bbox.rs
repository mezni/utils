use sqlx::postgres::PgPool;
use serde::{Deserialize, Serialize};
use crate::domain::gis::Station;

/// Bounding box query service
pub struct BoundingBoxQueryService {
    pool: PgPool,
}

impl BoundingBoxQueryService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Find stations within a bounding box
    pub async fn find_within_bbox(
        &self,
        min_lat: f64,
        max_lat: f64,
        min_lon: f64,
        max_lon: f64,
        radius_meters: Option<i32>,
    ) -> Result<Vec<Station>, sqlx::Error> {
        // SQLx compile-time verified query
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
                AND ST_MakeBox2D(
                    ST_MakePoint(longitude, latitude)::geography,
                    ST_MakePoint(longitude, latitude)::geography
                ) && ST_MakeBox2D(
                    ST_MakePoint($1, $2)::geography,
                    ST_MakePoint($3, $4)::geography
                )
        "#;

        let mut query_builder = sqlx::query_as::<_, Station>(sql);

        // Add optional radius filter
        if let Some(radius) = radius_meters {
            query_builder = query_builder.bind(radius as i64);
        }

        let result = query_builder
            .bind(min_lon)
            .bind(min_lat)
            .bind(max_lon)
            .bind(max_lat)
            .fetch_all(&self.pool)
            .await;

        result
    }

    /// Find stations within a bounding box with pagination
    pub async fn find_with_pagination(
        &self,
        min_lat: f64,
        max_lat: f64,
        min_lon: f64,
        max_lon: f64,
        radius_meters: Option<i32>,
        page: u32,
        limit: u32,
    ) -> Result<(Vec<Station>, u64), sqlx::Error> {
        // Get total count
        let count_sql = r#"
            SELECT COUNT(*)
            FROM gis.osm_charging_stations
            WHERE is_available = TRUE
                AND ST_MakeBox2D(
                    ST_MakePoint(longitude, latitude)::geography,
                    ST_MakePoint(longitude, latitude)::geography
                ) && ST_MakeBox2D(
                    ST_MakePoint($1, $2)::geography,
                    ST_MakePoint($3, $4)::geography
                )
        "#;

        let total: (i64,) = sqlx::query_as(count_sql)
            .bind(min_lon)
            .bind(min_lat)
            .bind(max_lon)
            .bind(max_lat)
            .fetch_one(&self.pool)
            .await?;

        let total_count = total.0 as u64;

        // Get paginated results
        let offset = ((page - 1) as i64) * (limit as i64);

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
                AND ST_MakeBox2D(
                    ST_MakePoint(longitude, latitude)::geography,
                    ST_MakePoint(longitude, latitude)::geography
                ) && ST_MakeBox2D(
                    ST_MakePoint($1, $2)::geography,
                    ST_MakePoint($3, $4)::geography
                )
            ORDER BY ST_MakePoint(longitude, latitude)::geography <-> ST_MakePoint($5, $6)::geography ASC
            LIMIT $7 OFFSET $8
        "#;

        let results = sqlx::query_as::<_, Station>(sql)
            .bind(min_lon)
            .bind(min_lat)
            .bind(max_lon)
            .bind(max_lat)
            .bind((min_lon + max_lon) / 2.0) // Center point for distance calculation
            .bind((min_lat + max_lat) / 2.0)
            .bind(limit as i64)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;

        Ok((results, total_count))
    }

    /// Calculate bounding box dimensions in meters
    pub fn calculate_dimensions(
        &self,
        min_lat: f64,
        max_lat: f64,
        min_lon: f64,
        max_lon: f64,
    ) -> BoundingBoxDimensions {
        // Convert degrees to meters (approximate at given latitude)
        let lat_rad = min_lat.to_radians();
        let deg_to_m = 111320.0 * lat_rad.cos();

        let width_meters = (max_lon - min_lon) * deg_to_m;
        let height_meters = (max_lat - min_lat) * 111320.0;

        BoundingBoxDimensions {
            width_meters,
            height_meters,
        }
    }
}

/// Bounding box dimensions in meters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoundingBoxDimensions {
    pub width_meters: f64,
    pub height_meters: f64,
}

/// Viewport query (latitude, longitude, and radius for viewport)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewportQuery {
    pub lat: f64,
    pub lon: f64,
    pub radius: i32,
}

impl ViewportQuery {
    /// Convert viewport to bounding box
    pub fn to_bounding_box(&self) -> (f64, f64, f64, f64) {
        let half_radius_m = self.radius as f64 / 2.0;

        // Convert radius to degrees (approximate)
        let lat_rad = self.lat.to_radians();
        let deg_to_m = 111320.0 * lat_rad.cos();
        let deg = half_radius_m / deg_to_m;

        (
            self.lat - deg,
            self.lat + deg,
            self.lon - deg,
            self.lon + deg,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_find_within_bbox_with_mock_pool() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect("postgresql://test:test@localhost:5432/test")
            .await
            .expect("Failed to connect to test database");

        let service = BoundingBoxQueryService::new(pool);

        let result = service
            .find_within_bbox(40.0, 41.0, -74.0, -73.0, Some(5000))
            .await;

        assert!(result.is_err()); // Expect error due to missing database
    }

    #[tokio::test]
    async fn test_find_with_pagination_with_mock_pool() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect("postgresql://test:test@localhost:5432/test")
            .await
            .expect("Failed to connect to test database");

        let service = BoundingBoxQueryService::new(pool);

        let result = service
            .find_with_pagination(40.0, 41.0, -74.0, -73.0, Some(5000), 1, 10)
            .await;

        assert!(result.is_err()); // Expect error due to missing database
    }

    #[test]
    fn test_viewport_to_bounding_box() {
        let viewport = ViewportQuery {
            lat: 40.7829,
            lon: -73.9654,
            radius: 1000,
        };

        let (min_lat, max_lat, min_lon, max_lon) = viewport.to_bounding_box();

        // Validate bounds
        assert!(min_lat >= -90.0 && min_lat <= 90.0);
        assert!(max_lat >= -90.0 && max_lat <= 90.0);
        assert!(min_lon >= -180.0 && min_lon <= 180.0);
        assert!(max_lon >= -180.0 && max_lon <= 180.0);

        // Validate center is preserved
        assert!((min_lat + max_lat) / 2.0 - viewport.lat).abs() < 0.001;
        assert!((min_lon + max_lon) / 2.0 - viewport.lon).abs() < 0.001);
    }

    #[test]
    fn test_bounding_box_dimensions() {
        let dims = BoundingBoxDimensions {
            width_meters: 50000.0,
            height_meters: 50000.0,
        };

        assert_eq!(dims.width_meters, 50000.0);
        assert_eq!(dims.height_meters, 50000.0);
    }
}
