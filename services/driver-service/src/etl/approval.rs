use sqlx::postgres::PgPool;
use serde::{Deserialize, Serialize};
use crate::domain::gis::Station;

/// ETL approval service
pub struct ApprovalService {
    pool: PgPool,
}

impl ApprovalService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Approve a station for the curated table
    pub async fn approve_station(
        &self,
        station_id: &str,
    ) -> Result<ApprovalResult, sqlx::Error> {
        // Check if station exists
        let exists: (bool,) = sqlx::query_as(
            "SELECT EXISTS(SELECT 1 FROM gis.osm_charging_stations WHERE id = $1)"
        )
        .bind(station_id)
        .fetch_one(&self.pool)
        .await?;

        if !exists.0 {
            return Err(sqlx::Error::Database(sqlx::error::DatabaseErrorKind::UniqueViolation(
                "Station not found".to_string(),
            )));
        }

        // Update station approval status
        let result = sqlx::query(
            "UPDATE gis.osm_charging_stations SET is_available = TRUE WHERE id = $1"
        )
        .bind(station_id)
        .execute(&self.pool)
        .await?;

        Ok(ApprovalResult {
            station_id: station_id.to_string(),
            approved: true,
            rows_affected: result.rows_affected(),
        })
    }

    /// Reject a station
    pub async fn reject_station(
        &self,
        station_id: &str,
    ) -> Result<ApprovalResult, sqlx::Error> {
        // Check if station exists
        let exists: (bool,) = sqlx::query_as(
            "SELECT EXISTS(SELECT 1 FROM gis.osm_charging_stations WHERE id = $1)"
        )
        .bind(station_id)
        .fetch_one(&self.pool)
        .await?;

        if !exists.0 {
            return Err(sqlx::Error::Database(sqlx::error::DatabaseErrorKind::UniqueViolation(
                "Station not found".to_string(),
            )));
        }

        // Update station approval status
        let result = sqlx::query(
            "UPDATE gis.osm_charging_stations SET is_available = FALSE WHERE id = $1"
        )
        .bind(station_id)
        .execute(&self.pool)
        .await?;

        Ok(ApprovalResult {
            station_id: station_id.to_string(),
            approved: false,
            rows_affected: result.rows_affected(),
        })
    }

    /// Get approval status for a station
    pub async fn get_approval_status(
        &self,
        station_id: &str,
    ) -> Result<ApprovalStatus, sqlx::Error> {
        let status: (bool,) = sqlx::query_as(
            "SELECT is_available FROM gis.osm_charging_stations WHERE id = $1"
        )
        .bind(station_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(ApprovalStatus {
            station_id: station_id.to_string(),
            approved: status.0,
        })
    }

    /// Get all unapproved stations
    pub async fn get_unapproved_stations(
        &self,
        limit: i64,
    ) -> Result<Vec<Station>, sqlx::Error> {
        let stations = sqlx::query_as::<_, Station>(
            "SELECT id, station_name as name, latitude, longitude, amenity, power, connector_types, is_available, last_updated, created_at FROM gis.osm_charging_stations WHERE is_available = FALSE LIMIT $1"
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(stations)
    }

    /// Approve all stations marked for approval
    pub async fn approve_all(&self) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(
            "UPDATE gis.osm_charging_stations SET is_available = TRUE WHERE is_available = FALSE"
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    /// Reject all stations marked for rejection
    pub async fn reject_all(&self) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(
            "UPDATE gis.osm_charging_stations SET is_available = FALSE WHERE is_available = TRUE"
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    /// Get approval statistics
    pub async fn get_approval_stats(&self) -> Result<ApprovalStats, sqlx::Error> {
        let stats: (i64, i64) = sqlx::query_as(
            "SELECT COUNT(*) FILTER (WHERE is_available = TRUE), COUNT(*) FILTER (WHERE is_available = FALSE) FROM gis.osm_charging_stations"
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(ApprovalStats {
            approved_count: stats.0,
            rejected_count: stats.1,
            total_count: stats.0 + stats.1,
        })
    }
}

/// Approval result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalResult {
    pub station_id: String,
    pub approved: bool,
    pub rows_affected: u64,
}

/// Approval status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalStatus {
    pub station_id: String,
    pub approved: bool,
}

/// Approval statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalStats {
    pub approved_count: i64,
    pub rejected_count: i64,
    pub total_count: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_approval_stats_with_mock_pool() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect("postgresql://test:test@localhost:5432/test")
            .await
            .expect("Failed to connect to test database");

        let service = ApprovalService::new(pool);

        let result = service.get_approval_stats().await;
        assert!(result.is_ok());

        let stats = result.unwrap();
        assert_eq!(stats.approved_count, 0);
        assert_eq!(stats.rejected_count, 0);
    }
}
