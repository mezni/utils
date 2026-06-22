use sqlx::postgres::PgPool;
use std::collections::HashSet;
use crate::telemetry::ingestion::OsmError;

/// Deduplication service for OSM ingestion
pub struct DeduplicationService {
    pool: PgPool,
}

impl DeduplicationService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Check for duplicate OSM IDs
    pub async fn check_duplicates(&self, osm_ids: &[i64]) -> Result<HashSet<i64>, sqlx::Error> {
        if osm_ids.is_empty() {
            return Ok(HashSet::new());
        }

        let placeholders: Vec<String> = osm_ids.iter().map(|_| "?".to_string()).collect();
        let placeholders_str = placeholders.join(",");

        let sql = format!(
            "SELECT osm_id FROM gis.osm_charging_stations_temp WHERE osm_id IN ({})",
            placeholders_str
        );

        let existing_ids: Vec<i64> = sqlx::query_as(&sql)
            .bind_all(osm_ids)
            .fetch_all(&self.pool)
            .await?;

        Ok(existing_ids.into_iter().collect())
    }

    /// Generate idempotency key for OSM record
    pub fn generate_idempotency_key(&self, osm_id: i64) -> String {
        format!("osm:ingest:{}", osm_id)
    }

    /// Check if record should be ingested (deterministic check)
    pub async fn should_ingest(&self, osm_id: i64) -> Result<bool, sqlx::Error> {
        // Check if OSM ID already exists in staging
        let exists: (bool,) = sqlx::query_as(
            "SELECT EXISTS(SELECT 1 FROM gis.osm_charging_stations_temp WHERE osm_id = $1)"
        )
        .bind(osm_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(!exists.0)
    }

    /// Validate idempotency for batch ingestion
    pub async fn validate_idempotency_batch(
        &self,
        osm_ids: &[i64],
    ) -> Result<DeduplicationCheck, OsmError> {
        let existing = self.check_duplicates(osm_ids).await?;

        let duplicates: Vec<i64> = existing.into_iter().collect();
        let unique = osm_ids
            .iter()
            .filter(|id| !existing.contains(id))
            .copied()
            .collect();

        Ok(DeduplicationCheck {
            total: osm_ids.len(),
            duplicates: duplicates.len(),
            unique,
            should_ingest: !unique.is_empty(),
        })
    }

    /// Get statistics about deduplication
    pub async fn get_deduplication_stats(&self) -> Result<DedupStats, sqlx::Error> {
        let stats: (i64, i64) = sqlx::query_as(
            "SELECT COUNT(*) as total, COUNT(DISTINCT osm_id) as unique_osm_ids FROM gis.osm_charging_stations_temp"
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(DedupStats {
            total_records: stats.0,
            unique_records: stats.1,
            duplicate_ratio: if stats.0 > 0 {
                ((stats.0 - stats.1) as f64 / stats.0 as f64) * 100.0
            } else {
                0.0
            },
        })
    }

    /// Remove duplicates (keep latest record)
    pub async fn remove_duplicates(&self) -> Result<u64, sqlx::Error> {
        // This is a simplified implementation
        // In production, you would need to:
        // 1. Find records with same osm_id
        // 2. Keep the one with latest import_timestamp
        // 3. Delete the others

        let result = sqlx::query(
            "DELETE FROM gis.osm_charging_stations_temp AS t1 USING gis.osm_charging_stations_temp AS t2 WHERE t1.osm_id = t2.osm_id AND t1.import_timestamp < t2.import_timestamp"
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }
}

/// Result of deduplication check
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeduplicationCheck {
    pub total: usize,
    pub duplicates: usize,
    pub unique: Vec<i64>,
    pub should_ingest: bool,
}

/// Deduplication statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupStats {
    pub total_records: i64,
    pub unique_records: i64,
    pub duplicate_ratio: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_generate_idempotency_key() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect("postgresql://test:test@localhost:5432/test")
            .await
            .expect("Failed to connect to test database");

        let service = DeduplicationService::new(pool);

        // Test key generation
        let key = service.generate_idempotency_key(123456789);
        assert_eq!(key, "osm:ingest:123456789");
    }

    #[tokio::test]
    async fn test_validate_idempotency_batch_with_mock_pool() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect("postgresql://test:test@localhost:5432/test")
            .await
            .expect("Failed to connect to test database");

        let service = DeduplicationService::new(pool);

        let result = service.validate_idempotency_batch(&[123, 456, 789]).await;
        assert!(result.is_ok());

        let check = result.unwrap();
        assert_eq!(check.total, 3);
        assert_eq!(check.should_ingest, true);
    }

    #[test]
    fn test_dedup_stats() {
        let stats = DedupStats {
            total_records: 100,
            unique_records: 80,
            duplicate_ratio: 20.0,
        };

        assert_eq!(stats.total_records, 100);
        assert_eq!(stats.unique_records, 80);
        assert_eq!(stats.duplicate_ratio, 20.0);
    }
}
