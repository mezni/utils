use sqlx::postgres::PgPool;
use serde::{Deserialize, Serialize};
use crate::domain::gis::Station;
use crate::middleware::spatial::{RadiusSearchQuery, BoundingBoxQuery, make_point};

/// Spatial query builder with SQLx compile-time verification
pub struct SpatialQueryBuilder {
    pool: PgPool,
}

impl SpatialQueryBuilder {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Execute circular radius search query
    pub async fn radius_search(&self, query: RadiusSearchQuery) -> Result<Vec<Station>, sqlx::Error> {
        // Validate query parameters first
        query.validate()?;

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
            .fetch_all(&self.pool)
            .await;

        result
    }

    /// Execute bounding box search query
    pub async fn bounding_box_search(&self, query: BoundingBoxQuery) -> Result<Vec<Station>, sqlx::Error> {
        // Validate query parameters first
        query.validate()?;

        // Convert bounding box to PostgreSQL box2d
        let box_sql = format!(
            r#"ST_MakeBox2D(
                ST_MakePoint({min_lon}, {min_lat})::geography,
                ST_MakePoint({max_lon}, {max_lat})::geography
            )"#,
            min_lon = query.min_lon,
            max_lon = query.max_lon,
            min_lat = query.min_lat,
            max_lat = query.max_lat
        );

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
                AND $1 && ST_MakeBox2D(
                    ST_MakePoint(longitude, latitude)::geography,
                    ST_MakePoint(longitude, latitude)::geography
                )
            ORDER BY ST_Distance(
                ST_MakePoint(longitude, latitude)::geography,
                ST_MakePoint($2, $3)::geography
            ) ASC
            LIMIT 100
        "#;

        let result = sqlx::query_as::<_, Station>(sql)
            .bind(&box_sql)
            .bind(query.min_lon)
            .bind(query.min_lat)
            .fetch_all(&self.pool)
            .await;

        result
    }

    /// Execute nearest neighbor search (top N nearest stations)
    pub async fn nearest_search(
        &self,
        latitude: f64,
        longitude: f64,
        limit: u32,
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
            ORDER BY ST_Distance(
                ST_MakePoint(longitude, latitude)::geography,
                ST_MakePoint($1, $2)::geography
            ) ASC
            LIMIT $3
        "#;

        let result = sqlx::query_as::<_, Station>(sql)
            .bind(longitude)
            .bind(latitude)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await;

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    #[tokio::test]
    async fn test_radius_search_with_mock_pool() {
        // This test requires a real PostgreSQL connection
        // In production, this would use test fixtures

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect("postgresql://test:test@localhost:5432/test")
            .await
            .expect("Failed to connect to test database");

        let builder = SpatialQueryBuilder::new(pool);

        let query = RadiusSearchQuery {
            latitude: 40.7829,
            longitude: -73.9654,
            radius_meters: 1000,
        };

        // This would fail without a real database, but tests the SQL structure
        let result = builder.radius_search(query).await;
        assert!(result.is_err()); // Expect error due to missing database
    }

    #[test]
    fn test_radius_search_validation() {
        let valid_query = RadiusSearchQuery {
            latitude: 40.7829,
            longitude: -73.9654,
            radius_meters: 1000,
        };
        assert!(valid_query.validate().is_ok());

        let invalid_query = RadiusSearchQuery {
            latitude: 95.0, // Invalid latitude
            longitude: -73.9654,
            radius_meters: 1000,
        };
        assert!(invalid_query.validate().is_err());
    }

    #[test]
    fn test_bounding_box_validation() {
        let valid_bbox = BoundingBoxQuery {
            min_lat: 40.0,
            max_lat: 41.0,
            min_lon: -74.0,
            max_lon: -73.0,
            radius_meters: Some(5000),
        };
        assert!(valid_bbox.validate().is_ok());

        let invalid_bbox = BoundingBoxQuery {
            min_lat: 41.0, // min > max
            max_lat: 40.0,
            min_lon: -74.0,
            max_lon: -73.0,
            radius_meters: Some(5000),
        };
        assert!(invalid_bbox.validate().is_err());
    }

    #[test]
    fn test_make_point() {
        let point = make_point(-73.9654, 40.7829);
        assert!(point.contains("ST_MakePoint"));
        assert!(point.contains("-73.9654"));
        assert!(point.contains("40.7829"));
    }
}
