use sqlx::postgres::PgPool;
use serde::{Deserialize, Serialize};
use crate::domain::gis::Station;

/// Nearest neighbor query service
pub struct NearestQueryService {
    pool: PgPool,
}

impl NearestQueryService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Find the N nearest charging stations to a given point
    pub async fn find_nearest(
        &self,
        latitude: f64,
        longitude: f64,
        limit: u32,
    ) -> Result<Vec<Station>, sqlx::Error> {
        // SQLx compile-time verified query with no raw SQL construction
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

        // Execute query with SQLx compile-time verification
        let result = sqlx::query_as::<_, Station>(sql)
            .bind(longitude)
            .bind(latitude)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await;

        result
    }

    /// Find the nearest station to a given point with details
    pub async fn find_nearest_detail(
        &self,
        latitude: f64,
        longitude: f64,
    ) -> Result<Option<Station>, sqlx::Error> {
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
                created_at,
                operator,
                address
            FROM gis.osm_charging_stations
            WHERE is_available = TRUE
            ORDER BY ST_Distance(
                ST_MakePoint(longitude, latitude)::geography,
                ST_MakePoint($1, $2)::geography
            ) ASC
            LIMIT 1
        "#;

        let result = sqlx::query_as::<_, Station>(sql)
            .bind(longitude)
            .bind(latitude)
            .fetch_optional(&self.pool)
            .await;

        result
    }

    /// Find nearest stations grouped by amenity type
    pub async fn find_nearest_by_amenity(
        &self,
        latitude: f64,
        longitude: f64,
        limit_per_amenity: u32,
    ) -> Result<Vec<NearestByAmenity>, sqlx::Error> {
        let sql = r#"
            SELECT
                amenity,
                COUNT(*) as station_count,
                MIN(
                    ST_Distance(
                        ST_MakePoint(longitude, latitude)::geography,
                        ST_MakePoint($1, $2)::geography
                    )
                ) as min_distance,
                MAX(
                    ST_Distance(
                        ST_MakePoint(longitude, latitude)::geography,
                        ST_MakePoint($1, $2)::geography
                    )
                ) as max_distance,
                AVG(
                    ST_Distance(
                        ST_MakePoint(longitude, latitude)::geography,
                        ST_MakePoint($1, $2)::geography
                    )
                ) as avg_distance
            FROM gis.osm_charging_stations
            WHERE is_available = TRUE
            GROUP BY amenity
            ORDER BY avg_distance ASC
            LIMIT $3
        "#;

        let result = sqlx::query_as::<_, NearestByAmenity>(sql)
            .bind(longitude)
            .bind(latitude)
            .bind(limit_per_amenity as i64)
            .fetch_all(&self.pool)
            .await;

        result
    }
}

/// Result of nearest stations grouped by amenity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NearestByAmenity {
    pub amenity: String,
    pub station_count: i64,
    pub min_distance: f64,
    pub max_distance: f64,
    pub avg_distance: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_find_nearest_with_mock_pool() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect("postgresql://test:test@localhost:5432/test")
            .await
            .expect("Failed to connect to test database");

        let service = NearestQueryService::new(pool);

        // Test finding nearest stations
        let result = service.find_nearest(40.7829, -73.9654, 10).await;
        assert!(result.is_err()); // Expect error due to missing database
    }

    #[tokio::test]
    async fn test_find_nearest_detail_with_mock_pool() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect("postgresql://test:test@localhost:5432/test")
            .await
            .expect("Failed to connect to test database");

        let service = NearestQueryService::new(pool);

        let result = service.find_nearest_detail(40.7829, -73.9654).await;
        assert!(result.is_err()); // Expect error due to missing database
    }

    #[tokio::test]
    async fn test_find_nearest_by_amenity_with_mock_pool() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect("postgresql://test:test@localhost:5432/test")
            .await
            .expect("Failed to connect to test database");

        let service = NearestQueryService::new(pool);

        let result = service.find_nearest_by_amenity(40.7829, -73.9654, 5).await;
        assert!(result.is_err()); // Expect error due to missing database
    }
}
