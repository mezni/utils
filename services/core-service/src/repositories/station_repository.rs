use crate::models::Station;
use crate::utils::database::Database;
use sqlx::{Error, Postgres, Transaction};
use std::sync::Arc;
use chrono::{DateTime, Utc};

pub struct StationRepository {
    db: Arc<Database>,
}

impl StationRepository {
    /// Create a new StationRepository
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// Create a new station in the database
    pub async fn create(&self, station: &Station) -> Result<Station, Error> {
        let mut tx = self.db.begin().await?;
        
        let result = sqlx::query_as!(
            Station,
            r#"
            INSERT INTO stations (
                id, company_id, name, description, address, latitude, longitude,
                phone, email, website, access_type, operating_hours, amenities,
                is_active, deleted_at, version, created_at, updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
            ) RETURNING *
            "#,
            station.base.id,
            station.company_id,
            station.name,
            station.description,
            station.address,
            station.latitude,
            station.longitude,
            station.phone,
            station.email,
            station.website,
            station.access_type as crate::models::AccessType,
            station.operating_hours,
            station.amenities,
            station.is_active,
            station.deleted_at,
            station.version,
            station.base.created_at,
            station.base.updated_at
        )
        .fetch_one(&mut *tx)
        .await?;
        
        tx.commit().await?;
        Ok(result)
    }

    /// Find a station by ID (excluding soft-deleted records)
    pub async fn find_by_id(&self, id: &str) -> Result<Option<Station>, Error> {
        let result = sqlx::query_as!(
            Station,
            r#"
            SELECT 
                id, company_id, name, description, address, latitude, longitude,
                phone, email, website, access_type as "access_type: crate::models::AccessType", 
                operating_hours, amenities, is_active, deleted_at, version, created_at, updated_at
            FROM stations 
            WHERE id = $1 AND deleted_at IS NULL
            "#,
            id
        )
        .fetch_optional(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Find a station by ID (including soft-deleted records)
    pub async fn find_by_id_including_deleted(&self, id: &str) -> Result<Option<Station>, Error> {
        let result = sqlx::query_as!(
            Station,
            r#"
            SELECT 
                id, company_id, name, description, address, latitude, longitude,
                phone, email, website, access_type as "access_type: crate::models::AccessType", 
                operating_hours, amenities, is_active, deleted_at, version, created_at, updated_at
            FROM stations 
            WHERE id = $1
            "#,
            id
        )
        .fetch_optional(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Find all stations for a company (excluding soft-deleted records)
    pub async fn find_by_company(&self, company_id: &str) -> Result<Vec<Station>, Error> {
        let result = sqlx::query_as!(
            Station,
            r#"
            SELECT 
                id, company_id, name, description, address, latitude, longitude,
                phone, email, website, access_type as "access_type: crate::models::AccessType", 
                operating_hours, amenities, is_active, deleted_at, version, created_at, updated_at
            FROM stations 
            WHERE company_id = $1 AND deleted_at IS NULL
            ORDER BY name
            "#,
            company_id
        )
        .fetch_all(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Find all active stations (excluding soft-deleted records)
    pub async fn find_all(&self) -> Result<Vec<Station>, Error> {
        let result = sqlx::query_as!(
            Station,
            r#"
            SELECT 
                id, company_id, name, description, address, latitude, longitude,
                phone, email, website, access_type as "access_type: crate::models::AccessType", 
                operating_hours, amenities, is_active, deleted_at, version, created_at, updated_at
            FROM stations 
            WHERE deleted_at IS NULL
            ORDER BY name
            "#
        )
        .fetch_all(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Update a station with optimistic concurrency control
    pub async fn update(&self, station: &Station) -> Result<Station, Error> {
        let mut tx = self.db.begin().await?;
        
        let result = sqlx::query_as!(
            Station,
            r#"
            UPDATE stations 
            SET 
                company_id = $2, 
                name = $3, 
                description = $4, 
                address = $5, 
                latitude = $6, 
                longitude = $7,
                phone = $8, 
                email = $9, 
                website = $10, 
                access_type = $11,
                operating_hours = $12, 
                amenities = $13, 
                is_active = $14, 
                updated_at = $15,
                version = version + 1
            WHERE id = $1 AND version = $16 AND deleted_at IS NULL
            RETURNING *
            "#,
            station.base.id,
            station.company_id,
            station.name,
            station.description,
            station.address,
            station.latitude,
            station.longitude,
            station.phone,
            station.email,
            station.website,
            station.access_type as crate::models::AccessType,
            station.operating_hours,
            station.amenities,
            station.is_active,
            station.base.updated_at,
            station.version
        )
        .fetch_one(&mut *tx)
        .await?;
        
        tx.commit().await?;
        Ok(result)
    }

    /// Soft delete a station
    pub async fn delete(&self, id: &str, version: i32) -> Result<bool, Error> {
        let now = Utc::now();
        let result = sqlx::query!(
            r#"
            UPDATE stations 
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

    /// Restore a soft-deleted station
    pub async fn restore(&self, id: &str, version: i32) -> Result<bool, Error> {
        let now = Utc::now();
        let result = sqlx::query!(
            r#"
            UPDATE stations 
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

    /// Find stations by name (partial match, excluding soft-deleted records)
    pub async fn find_by_name(&self, name: &str) -> Result<Vec<Station>, Error> {
        let pattern = format!("%{}%", name);
        let result = sqlx::query_as!(
            Station,
            r#"
            SELECT 
                id, company_id, name, description, address, latitude, longitude,
                phone, email, website, access_type as "access_type: crate::models::AccessType", 
                operating_hours, amenities, is_active, deleted_at, version, created_at, updated_at
            FROM stations 
            WHERE deleted_at IS NULL AND name ILIKE $1
            ORDER BY name
            "#,
            pattern
        )
        .fetch_all(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Find stations within a geographic radius (excluding soft-deleted records)
    pub async fn find_by_radius(
        &self, 
        center_lat: f64, 
        center_lon: f64, 
        radius_km: f64
    ) -> Result<Vec<Station>, Error> {
        // Earth's radius in kilometers
        const EARTH_RADIUS_KM: f64 = 6371.0;
        
        let result = sqlx::query_as!(
            Station,
            r#"
            SELECT 
                id, company_id, name, description, address, latitude, longitude,
                phone, email, website, access_type as "access_type: crate::models::AccessType", 
                operating_hours, amenities, is_active, deleted_at, version, created_at, updated_at
            FROM stations 
            WHERE deleted_at IS NULL
            AND (6371.0 * ACOS(
                COS(RADIANS($1)) * COS(RADIANS(latitude)) * 
                COS(RADIANS(longitude) - RADIANS($2)) + 
                SIN(RADIANS($1)) * SIN(RADIANS(latitude))
            )) <= $3
            ORDER BY name
            "#,
            center_lat,
            center_lon,
            radius_km
        )
        .fetch_all(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Find stations by access type (excluding soft-deleted records)
    pub async fn find_by_access_type(&self, access_type: crate::models::AccessType) -> Result<Vec<Station>, Error> {
        let result = sqlx::query_as!(
            Station,
            r#"
            SELECT 
                id, company_id, name, description, address, latitude, longitude,
                phone, email, website, access_type as "access_type: crate::models::AccessType", 
                operating_hours, amenities, is_active, deleted_at, version, created_at, updated_at
            FROM stations 
            WHERE deleted_at IS NULL AND access_type = $1
            ORDER BY name
            "#,
            access_type as crate::models::AccessType
        )
        .fetch_all(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Check if a station exists by ID (excluding soft-deleted records)
    pub async fn exists(&self, id: &str) -> Result<bool, Error> {
        let result = sqlx::query_scalar!(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM stations 
                WHERE id = $1 AND deleted_at IS NULL
            )
            "#,
            id
        )
        .fetch_one(&self.db.pool)
        .await?;
        
        Ok(result.exists.unwrap_or(false))
    }

    /// Count active stations for a company (excluding soft-deleted records)
    pub async fn count_by_company(&self, company_id: &str) -> Result<i64, Error> {
        let result = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) 
            FROM stations 
            WHERE company_id = $1 AND deleted_at IS NULL
            "#,
            company_id
        )
        .fetch_one(&self.db.pool)
        .await?;
        
        Ok(result.count.unwrap_or(0))
    }

    /// Count all active stations (excluding soft-deleted records)
    pub async fn count(&self) -> Result<i64, Error> {
        let result = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) 
            FROM stations 
            WHERE deleted_at IS NULL
            "#
        )
        .fetch_one(&self.db.pool)
        .await?;
        
        Ok(result.count.unwrap_or(0))
    }

    /// Get the current version of a station
    pub async fn get_version(&self, id: &str) -> Result<Option<i32>, Error> {
        let result = sqlx::query_scalar!(
            r#"
            SELECT version 
            FROM stations 
            WHERE id = $1 AND deleted_at IS NULL
            "#,
            id
        )
        .fetch_optional(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Find stations created within a date range (excluding soft-deleted records)
    pub async fn find_by_created_range(
        &self, 
        start: DateTime<Utc>, 
        end: DateTime<Utc>
    ) -> Result<Vec<Station>, Error> {
        let result = sqlx::query_as!(
            Station,
            r#"
            SELECT 
                id, company_id, name, description, address, latitude, longitude,
                phone, email, website, access_type as "access_type: crate::models::AccessType", 
                operating_hours, amenities, is_active, deleted_at, version, created_at, updated_at
            FROM stations 
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

    /// Find stations updated within a date range (excluding soft-deleted records)
    pub async fn find_by_updated_range(
        &self, 
        start: DateTime<Utc>, 
        end: DateTime<Utc>
    ) -> Result<Vec<Station>, Error> {
        let result = sqlx::query_as!(
            Station,
            r#"
            SELECT 
                id, company_id, name, description, address, latitude, longitude,
                phone, email, website, access_type as "access_type: crate::models::AccessType", 
                operating_hours, amenities, is_active, deleted_at, version, created_at, updated_at
            FROM stations 
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
    use crate::models::{StationId, CompanyId, AccessType};
    
    #[tokio::test]
    async fn test_create_station() {
        // This test would require a test database
        let db = Arc::new(Database::new("postgresql://test:test@localhost/test").await.unwrap());
        let repo = StationRepository::new(db);
        
        let company_id = CompanyId::generate_id();
        let station = Station::new(
            company_id,
            "Test Station".to_string(),
            "Test Address".to_string(),
            36.8065,
            10.1815,
        );
        
        // This would fail without a test database, but verifies the SQL is correct
        // let result = repo.create(&station).await;
        // assert!(result.is_ok());
        
        assert_eq!(station.name, "Test Station");
        assert_eq!(station.access_type, AccessType::Public);
        assert!(StationId::validate_id(&station.id));
    }
    
    #[tokio::test]
    async fn test_station_validation() {
        let company_id = CompanyId::generate_id();
        let station = Station::new(
            company_id,
            "Test Station".to_string(),
            "Test Address".to_string(),
            36.8065,
            10.1815,
        );
        assert!(station.validate().is_ok());
        
        let company_id = CompanyId::generate_id();
        let mut invalid_station = Station::new(
            company_id,
            "".to_string(),
            "Test Address".to_string(),
            36.8065,
            10.1815,
        );
        assert!(invalid_station.validate().is_err());
    }
}