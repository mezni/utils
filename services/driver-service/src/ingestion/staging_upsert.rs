use sqlx::postgres::PgPool;
use serde::{Deserialize, Serialize};
use crate::domain::gis::Station;
use crate::ingestion::osm_parser::OsmTagNormalized;
use crate::telemetry::ingestion::IngestionJobStatus;

/// Staging upsert service for OSM data
pub struct StagingUpsertService {
    pool: PgPool,
}

impl StagingUpsertService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Upsert station data to staging table
    pub async fn upsert_staging(
        &self,
        station: OsmTagNormalized,
        osm_id: i64,
        import_timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<StagingResult, sqlx::Error> {
        // Serialize raw tags to JSONB
        let osm_data = serde_json::to_value(&station.tags).map_err(|e| {
            sqlx::Error::Boxed(Box::new(e))
        })?;

        // Generate station ID using nanoid pattern
        let station_id = self.generate_station_id(osm_id);

        // Create station data for staging
        let data = r#"
            INSERT INTO gis.osm_charging_stations_temp (
                id,
                osm_id,
                osm_data,
                import_timestamp,
                processed
            ) VALUES ($1, $2, $3, $4, FALSE)
            ON CONFLICT (osm_id)
            DO UPDATE SET
                osm_data = EXCLUDED.osm_data,
                import_timestamp = EXCLUDED.import_timestamp,
                processed = EXCLUDED.processed
            RETURNING id, osm_id, processed
        "#;

        let (id, retrieved_osm_id, processed): (String, i64, bool) = sqlx::query_as(data)
            .bind(station_id)
            .bind(osm_id)
            .bind(&osm_data)
            .bind(import_timestamp)
            .fetch_one(&self.pool)
            .await?;

        Ok(StagingResult {
            id,
            osm_id: retrieved_osm_id,
            processed,
            action: if retrieved_osm_id == osm_id {
                StagingAction::Updated
            } else {
                StagingAction::Inserted
            },
        })
    }

    /// Generate station ID using nanoid pattern (STA-XXXXXXXXXXXX)
    fn generate_station_id(&self, osm_id: i64) -> String {
        format!("STA-{}", nanoid::nanoid!(12))
    }

    /// Mark all staging records as processed
    pub async fn mark_all_processed(&self) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(
            "UPDATE gis.osm_charging_stations_temp SET processed = TRUE WHERE processed = FALSE",
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    /// Get unprocessed staging records
    pub async fn get_unprocessed(&self, limit: i64) -> Result<Vec<StagingRecord>, sqlx::Error> {
        let records = sqlx::query_as::<_, StagingRecord>(
            "SELECT id, osm_id, import_timestamp, processed FROM gis.osm_charging_stations_temp WHERE processed = FALSE LIMIT $1"
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Get processed staging records
    pub async fn get_processed(&self, limit: i64) -> Result<Vec<StagingRecord>, sqlx::Error> {
        let records = sqlx::query_as::<_, StagingRecord>(
            "SELECT id, osm_id, import_timestamp, processed FROM gis.osm_charging_stations_temp WHERE processed = TRUE LIMIT $1"
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Delete staging records by OSM ID
    pub async fn delete_by_osm_id(&self, osm_id: i64) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(
            "DELETE FROM gis.osm_charging_stations_temp WHERE osm_id = $1"
        )
        .bind(osm_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }
}

/// Result of staging upsert operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagingResult {
    pub id: String,
    pub osm_id: i64,
    pub processed: bool,
    pub action: StagingAction,
}

/// Action taken during staging
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum StagingAction {
    Inserted,
    Updated,
}

/// Staging record from database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagingRecord {
    pub id: String,
    pub osm_id: i64,
    pub import_timestamp: chrono::DateTime<chrono::Utc>,
    pub processed: bool,
}

/// Staging statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagingStats {
    pub total_records: i64,
    pub processed_records: i64,
    pub unprocessed_records: i64,
    pub insert_count: i64,
    pub update_count: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_generate_station_id() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect("postgresql://test:test@localhost:5432/test")
            .await
            .expect("Failed to connect to test database");

        let service = StagingUpsertService::new(pool);

        // Test ID generation
        let id = service.generate_station_id(123456789);
        assert!(id.starts_with("STA-"));
        assert_eq!(id.len(), 17); // "STA-" + 12 chars
    }

    #[tokio::test]
    async fn test_get_unprocessed_with_mock_pool() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect("postgresql://test:test@localhost:5432/test")
            .await
            .expect("Failed to connect to test database");

        let service = StagingUpsertService::new(pool);

        let result = service.get_unprocessed(10).await;
        assert!(result.is_err()); // Expect error due to missing database
    }
}
