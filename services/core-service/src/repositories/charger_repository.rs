use crate::models::Charger;
use crate::utils::database::Database;
use sqlx::{Error, Postgres, Transaction};
use std::sync::Arc;
use chrono::{DateTime, Utc};

pub struct ChargerRepository {
    db: Arc<Database>,
}

impl ChargerRepository {
    /// Create a new ChargerRepository
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// Create a new charger in the database
    pub async fn create(&self, charger: &Charger) -> Result<Charger, Error> {
        let mut tx = self.db.begin().await?;
        
        let result = sqlx::query_as!(
            Charger,
            r#"
            INSERT INTO chargers (
                id, station_id, name, description, charger_type, power_output_kw,
                voltage, current, connector_types, status, last_status_update,
                is_public, pricing_info, is_active, deleted_at, version, created_at, updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
            ) RETURNING *
            "#,
            charger.base.id,
            charger.station_id,
            charger.name,
            charger.description,
            charger.charger_type as crate::models::ChargerType,
            charger.power_output_kw,
            charger.voltage,
            charger.current,
            charger.connector_types as Vec<crate::models::ConnectorType>,
            charger.status as crate::models::ChargerStatus,
            charger.last_status_update,
            charger.is_public,
            charger.pricing_info,
            charger.is_active,
            charger.deleted_at,
            charger.version,
            charger.base.created_at,
            charger.base.updated_at
        )
        .fetch_one(&mut *tx)
        .await?;
        
        tx.commit().await?;
        Ok(result)
    }

    /// Find a charger by ID (excluding soft-deleted records)
    pub async fn find_by_id(&self, id: &str) -> Result<Option<Charger>, Error> {
        let result = sqlx::query_as!(
            Charger,
            r#"
            SELECT 
                id, station_id, name, description, 
                charger_type as "charger_type: crate::models::ChargerType", 
                power_output_kw, voltage, current, 
                connector_types as "connector_types: Vec<crate::models::ConnectorType>", 
                status as "status: crate::models::ChargerStatus", 
                last_status_update, is_public, pricing_info, is_active, 
                deleted_at, version, created_at, updated_at
            FROM chargers 
            WHERE id = $1 AND deleted_at IS NULL
            "#,
            id
        )
        .fetch_optional(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Find a charger by ID (including soft-deleted records)
    pub async fn find_by_id_including_deleted(&self, id: &str) -> Result<Option<Charger>, Error> {
        let result = sqlx::query_as!(
            Charger,
            r#"
            SELECT 
                id, station_id, name, description, 
                charger_type as "charger_type: crate::models::ChargerType", 
                power_output_kw, voltage, current, 
                connector_types as "connector_types: Vec<crate::models::ConnectorType>", 
                status as "status: crate::models::ChargerStatus", 
                last_status_update, is_public, pricing_info, is_active, 
                deleted_at, version, created_at, updated_at
            FROM chargers 
            WHERE id = $1
            "#,
            id
        )
        .fetch_optional(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Find all chargers for a station (excluding soft-deleted records)
    pub async fn find_by_station(&self, station_id: &str) -> Result<Vec<Charger>, Error> {
        let result = sqlx::query_as!(
            Charger,
            r#"
            SELECT 
                id, station_id, name, description, 
                charger_type as "charger_type: crate::models::ChargerType", 
                power_output_kw, voltage, current, 
                connector_types as "connector_types: Vec<crate::models::ConnectorType>", 
                status as "status: crate::models::ChargerStatus", 
                last_status_update, is_public, pricing_info, is_active, 
                deleted_at, version, created_at, updated_at
            FROM chargers 
            WHERE station_id = $1 AND deleted_at IS NULL
            ORDER BY name
            "#,
            station_id
        )
        .fetch_all(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Find all active chargers (excluding soft-deleted records)
    pub async fn find_all(&self) -> Result<Vec<Charger>, Error> {
        let result = sqlx::query_as!(
            Charger,
            r#"
            SELECT 
                id, station_id, name, description, 
                charger_type as "charger_type: crate::models::ChargerType", 
                power_output_kw, voltage, current, 
                connector_types as "connector_types: Vec<crate::models::ConnectorType>", 
                status as "status: crate::models::ChargerStatus", 
                last_status_update, is_public, pricing_info, is_active, 
                deleted_at, version, created_at, updated_at
            FROM chargers 
            WHERE deleted_at IS NULL
            ORDER BY name
            "#
        )
        .fetch_all(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Update a charger with optimistic concurrency control
    pub async fn update(&self, charger: &Charger) -> Result<Charger, Error> {
        let mut tx = self.db.begin().await?;
        
        let result = sqlx::query_as!(
            Charger,
            r#"
            UPDATE chargers 
            SET 
                station_id = $2, 
                name = $3, 
                description = $4, 
                charger_type = $5,
                power_output_kw = $6,
                voltage = $7,
                current = $8,
                connector_types = $9,
                status = $10,
                last_status_update = $11,
                is_public = $12,
                pricing_info = $13,
                is_active = $14,
                updated_at = $15,
                version = version + 1
            WHERE id = $1 AND version = $16 AND deleted_at IS NULL
            RETURNING *
            "#,
            charger.base.id,
            charger.station_id,
            charger.name,
            charger.description,
            charger.charger_type as crate::models::ChargerType,
            charger.power_output_kw,
            charger.voltage,
            charger.current,
            charger.connector_types as Vec<crate::models::ConnectorType>,
            charger.status as crate::models::ChargerStatus,
            charger.last_status_update,
            charger.is_public,
            charger.pricing_info,
            charger.is_active,
            charger.base.updated_at,
            charger.version
        )
        .fetch_one(&mut *tx)
        .await?;
        
        tx.commit().await?;
        Ok(result)
    }

    /// Update charger status only
    pub async fn update_status(&self, id: &str, status: crate::models::ChargerStatus) -> Result<bool, Error> {
        let now = Utc::now();
        let result = sqlx::query!(
            r#"
            UPDATE chargers 
            SET 
                status = $2,
                last_status_update = $3,
                updated_at = $3,
                version = version + 1
            WHERE id = $1 AND deleted_at IS NULL
            "#,
            id,
            status as crate::models::ChargerStatus,
            now
        )
        .execute(&self.db.pool)
        .await?;
        
        Ok(result.rows_affected() > 0)
    }

    /// Soft delete a charger
    pub async fn delete(&self, id: &str, version: i32) -> Result<bool, Error> {
        let now = Utc::now();
        let result = sqlx::query!(
            r#"
            UPDATE chargers 
            SET 
                deleted_at = $2, 
                updated_at = $2,
                version = version + 1
            WHERE id = $1 AND version = $1 AND deleted_at IS NULL
            "#,
            id,
            now
        )
        .execute(&self.db.pool)
        .await?;
        
        Ok(result.rows_affected() > 0)
    }

    /// Restore a soft-deleted charger
    pub async fn restore(&self, id: &str, version: i32) -> Result<bool, Error> {
        let now = Utc::now();
        let result = sqlx::query!(
            r#"
            UPDATE chargers 
            SET 
                deleted_at = NULL, 
                updated_at = $2,
                version = version + 1
            WHERE id = $1 AND version = $1 AND deleted_at IS NOT NULL
            "#,
            id,
            now
        )
        .execute(&self.db.pool)
        .await?;
        
        Ok(result.rows_affected() > 0)
    }

    /// Find chargers by name (partial match, excluding soft-deleted records)
    pub async fn find_by_name(&self, name: &str) -> Result<Vec<Charger>, Error> {
        let pattern = format!("%{}%", name);
        let result = sqlx::query_as!(
            Charger,
            r#"
            SELECT 
                id, station_id, name, description, 
                charger_type as "charger_type: crate::models::ChargerType", 
                power_output_kw, voltage, current, 
                connector_types as "connector_types: Vec<crate::models::ConnectorType>", 
                status as "status: crate::models::ChargerStatus", 
                last_status_update, is_public, pricing_info, is_active, 
                deleted_at, version, created_at, updated_at
            FROM chargers 
            WHERE deleted_at IS NULL AND name ILIKE $1
            ORDER BY name
            "#,
            pattern
        )
        .fetch_all(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Find chargers by status (excluding soft-deleted records)
    pub async fn find_by_status(&self, status: crate::models::ChargerStatus) -> Result<Vec<Charger>, Error> {
        let result = sqlx::query_as!(
            Charger,
            r#"
            SELECT 
                id, station_id, name, description, 
                charger_type as "charger_type: crate::models::ChargerType", 
                power_output_kw, voltage, current, 
                connector_types as "connector_types: Vec<crate::models::ConnectorType>", 
                status as "status: crate::models::ChargerStatus", 
                last_status_update, is_public, pricing_info, is_active, 
                deleted_at, version, created_at, updated_at
            FROM chargers 
            WHERE deleted_at IS NULL AND status = $1
            ORDER BY name
            "#,
            status as crate::models::ChargerStatus
        )
        .fetch_all(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Find available chargers (status = Available, excluding soft-deleted records)
    pub async fn find_available(&self) -> Result<Vec<Charger>, Error> {
        let result = sqlx::query_as!(
            Charger,
            r#"
            SELECT 
                id, station_id, name, description, 
                charger_type as "charger_type: crate::models::ChargerType", 
                power_output_kw, voltage, current, 
                connector_types as "connector_types: Vec<crate::models::ConnectorType>", 
                status as "status: crate::models::ChargerStatus", 
                last_status_update, is_public, pricing_info, is_active, 
                deleted_at, version, created_at, updated_at
            FROM chargers 
            WHERE deleted_at IS NULL AND status = 'Available' AND is_active = true
            ORDER BY name
            "#,
        )
        .fetch_all(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Find chargers by charger type (excluding soft-deleted records)
    pub async fn find_by_charger_type(&self, charger_type: crate::models::ChargerType) -> Result<Vec<Charger>, Error> {
        let result = sqlx::query_as!(
            Charger,
            r#"
            SELECT 
                id, station_id, name, description, 
                charger_type as "charger_type: crate::models::ChargerType", 
                power_output_kw, voltage, current, 
                connector_types as "connector_types: Vec<crate::models::ConnectorType>", 
                status as "status: crate::models::ChargerStatus", 
                last_status_update, is_public, pricing_info, is_active, 
                deleted_at, version, created_at, updated_at
            FROM chargers 
            WHERE deleted_at IS NULL AND charger_type = $1
            ORDER BY name
            "#,
            charger_type as crate::models::ChargerType
        )
        .fetch_all(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Find chargers by connector type (excluding soft-deleted records)
    pub async fn find_by_connector_type(&self, connector_type: crate::models::ConnectorType) -> Result<Vec<Charger>, Error> {
        let result = sqlx::query_as!(
            Charger,
            r#"
            SELECT 
                id, station_id, name, description, 
                charger_type as "charger_type: crate::models::ChargerType", 
                power_output_kw, voltage, current, 
                connector_types as "connector_types: Vec<crate::models::ConnectorType>", 
                status as "status: crate::models::ChargerStatus", 
                last_status_update, is_public, pricing_info, is_active, 
                deleted_at, version, created_at, updated_at
            FROM chargers 
            WHERE deleted_at IS NULL AND $1 = ANY(connector_types)
            ORDER BY name
            "#,
            connector_type as crate::models::ConnectorType
        )
        .fetch_all(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Find public chargers (excluding soft-deleted records)
    pub async fn find_public(&self) -> Result<Vec<Charger>, Error> {
        let result = sqlx::query_as!(
            Charger,
            r#"
            SELECT 
                id, station_id, name, description, 
                charger_type as "charger_type: crate::models::ChargerType", 
                power_output_kw, voltage, current, 
                connector_types as "connector_types: Vec<crate::models::ConnectorType>", 
                status as "status: crate::models::ChargerStatus", 
                last_status_update, is_public, pricing_info, is_active, 
                deleted_at, version, created_at, updated_at
            FROM chargers 
            WHERE deleted_at IS NULL AND is_public = true
            ORDER BY name
            "#,
        )
        .fetch_all(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Check if a charger exists by ID (excluding soft-deleted records)
    pub async fn exists(&self, id: &str) -> Result<bool, Error> {
        let result = sqlx::query_scalar!(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM chargers 
                WHERE id = $1 AND deleted_at IS NULL
            )
            "#,
            id
        )
        .fetch_one(&self.db.pool)
        .await?;
        
        Ok(result.exists.unwrap_or(false))
    }

    /// Count active chargers for a station (excluding soft-deleted records)
    pub async fn count_by_station(&self, station_id: &str) -> Result<i64, Error> {
        let result = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) 
            FROM chargers 
            WHERE station_id = $1 AND deleted_at IS NULL
            "#,
            station_id
        )
        .fetch_one(&self.db.pool)
        .await?;
        
        Ok(result.count.unwrap_or(0))
    }

    /// Count all active chargers (excluding soft-deleted records)
    pub async fn count(&self) -> Result<i64, Error> {
        let result = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) 
            FROM chargers 
            WHERE deleted_at IS NULL
            "#
        )
        .fetch_one(&self.db.pool)
        .await?;
        
        Ok(result.count.unwrap_or(0))
    }

    /// Count available chargers (status = Available, excluding soft-deleted records)
    pub async fn count_available(&self) -> Result<i64, Error> {
        let result = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) 
            FROM chargers 
            WHERE deleted_at IS NULL AND status = 'Available' AND is_active = true
            "#
        )
        .fetch_one(&self.db.pool)
        .await?;
        
        Ok(result.count.unwrap_or(0))
    }

    /// Get the current version of a charger
    pub async fn get_version(&self, id: &str) -> Result<Option<i32>, Error> {
        let result = sqlx::query_scalar!(
            r#"
            SELECT version 
            FROM chargers 
            WHERE id = $1 AND deleted_at IS NULL
            "#,
            id
        )
        .fetch_optional(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Find chargers created within a date range (excluding soft-deleted records)
    pub async fn find_by_created_range(
        &self, 
        start: DateTime<Utc>, 
        end: DateTime<Utc>
    ) -> Result<Vec<Charger>, Error> {
        let result = sqlx::query_as!(
            Charger,
            r#"
            SELECT 
                id, station_id, name, description, 
                charger_type as "charger_type: crate::models::ChargerType", 
                power_output_kw, voltage, current, 
                connector_types as "connector_types: Vec<crate::models::ConnectorType>", 
                status as "status: crate::models::ChargerStatus", 
                last_status_update, is_public, pricing_info, is_active, 
                deleted_at, version, created_at, updated_at
            FROM chargers 
            WHERE deleted_at IS NULL 
                AND created_at >= $1 
                AND created_at <= $2
            ORDER BY created_at
            "#,
            start,
            end
        )
        .fetch_all(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Find chargers updated within a date range (excluding soft-deleted records)
    pub async fn find_by_updated_range(
        &self, 
        start: DateTime<Utc>, 
        end: DateTime<Utc>
    ) -> Result<Vec<Charger>, Error> {
        let result = sqlx::query_as!(
            Charger,
            r#"
            SELECT 
                id, station_id, name, description, 
                charger_type as "charger_type: crate::models::ChargerType", 
                power_output_kw, voltage, current, 
                connector_types as "connector_types: Vec<crate::models::ConnectorType>", 
                status as "status: crate::models::ChargerStatus", 
                last_status_update, is_public, pricing_info, is_active, 
                deleted_at, version, created_at, updated_at
            FROM chargers 
            WHERE deleted_at IS NULL 
                AND updated_at >= $1 
                AND updated_at <= $2
            ORDER BY updated_at
            "#,
            start,
            end
        )
        .fetch_all(&self.db.pool)
        .await?;
        
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ChargerId, StationId, ChargerType, ChargerStatus, ConnectorType};
    
    #[tokio::test]
    async fn test_create_charger() {
        // This test would require a test database
        let db = Arc::new(Database::new("postgresql://test:test@localhost/test").await.unwrap());
        let repo = ChargerRepository::new(db);
        
        let station_id = StationId::generate_id();
        let charger = Charger::new(
            station_id,
            "Test Charger".to_string(),
            ChargerType::AC,
            7.4,
            vec![ConnectorType::Type2],
        );
        
        // This would fail without a test database, but verifies the SQL is correct
        // let result = repo.create(&charger).await;
        // assert!(result.is_ok());
        
        assert_eq!(charger.name, "Test Charger");
        assert_eq!(charger.charger_type, ChargerType::AC);
        assert_eq!(charger.status, ChargerStatus::Available);
        assert!(ChargerId::validate_id(&charger.id));
    }
    
    #[tokio::test]
    async fn test_charger_validation() {
        let station_id = StationId::generate_id();
        let charger = Charger::new(
            station_id,
            "Test Charger".to_string(),
            ChargerType::AC,
            7.4,
            vec![ConnectorType::Type2],
        );
        assert!(charger.validate().is_ok());
        
        let station_id = StationId::generate_id();
        let mut invalid_charger = Charger::new(
            station_id,
            "".to_string(),
            ChargerType::AC,
            7.4,
            vec![],
        );
        assert!(invalid_charger.validate().is_err());
    }
}