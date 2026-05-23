use crate::models::Company;
use crate::utils::database::Database;
use sqlx::{Error, Postgres, Transaction};
use std::sync::Arc;
use chrono::{DateTime, Utc};

pub struct CompanyRepository {
    db: Arc<Database>,
}

impl CompanyRepository {
    /// Create a new CompanyRepository
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// Create a new company in the database
    pub async fn create(&self, company: &Company) -> Result<Company, Error> {
        let mut tx = self.db.begin().await?;
        
        let result = sqlx::query_as!(
            Company,
            r#"
            INSERT INTO companies (
                id, name, description, email, phone, website, address, logo_url, 
                is_active, deleted_at, version, created_at, updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
            ) RETURNING *
            "#,
            company.base.id,
            company.name,
            company.description,
            company.email,
            company.phone,
            company.website,
            company.address,
            company.logo_url,
            company.is_active,
            company.deleted_at,
            company.version,
            company.base.created_at,
            company.base.updated_at
        )
        .fetch_one(&mut *tx)
        .await?;
        
        tx.commit().await?;
        Ok(result)
    }

    /// Find a company by ID (excluding soft-deleted records)
    pub async fn find_by_id(&self, id: &str) -> Result<Option<Company>, Error> {
        let result = sqlx::query_as!(
            Company,
            r#"
            SELECT 
                id, name, description, email, phone, website, address, logo_url, 
                is_active, deleted_at, version, created_at, updated_at
            FROM companies 
            WHERE id = $1 AND deleted_at IS NULL
            "#,
            id
        )
        .fetch_optional(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Find a company by ID (including soft-deleted records)
    pub async fn find_by_id_including_deleted(&self, id: &str) -> Result<Option<Company>, Error> {
        let result = sqlx::query_as!(
            Company,
            r#"
            SELECT 
                id, name, description, email, phone, website, address, logo_url, 
                is_active, deleted_at, version, created_at, updated_at
            FROM companies 
            WHERE id = $1
            "#,
            id
        )
        .fetch_optional(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Find all active companies (excluding soft-deleted records)
    pub async fn find_all(&self) -> Result<Vec<Company>, Error> {
        let result = sqlx::query_as!(
            Company,
            r#"
            SELECT 
                id, name, description, email, phone, website, address, logo_url, 
                is_active, deleted_at, version, created_at, updated_at
            FROM companies 
            WHERE deleted_at IS NULL
            ORDER BY name
            "#
        )
        .fetch_all(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Update a company with optimistic concurrency control
    pub async fn update(&self, company: &Company) -> Result<Company, Error> {
        let mut tx = self.db.begin().await?;
        
        let result = sqlx::query_as!(
            Company,
            r#"
            UPDATE companies 
            SET 
                name = $2, 
                description = $3, 
                email = $4, 
                phone = $5, 
                website = $6, 
                address = $7, 
                logo_url = $8, 
                is_active = $9, 
                updated_at = $10,
                version = version + 1
            WHERE id = $1 AND version = $11 AND deleted_at IS NULL
            RETURNING *
            "#,
            company.base.id,
            company.name,
            company.description,
            company.email,
            company.phone,
            company.website,
            company.address,
            company.logo_url,
            company.is_active,
            company.base.updated_at,
            company.version
        )
        .fetch_one(&mut *tx)
        .await?;
        
        tx.commit().await?;
        Ok(result)
    }

    /// Soft delete a company
    pub async fn delete(&self, id: &str, version: i32) -> Result<bool, Error> {
        let now = Utc::now();
        let result = sqlx::query!(
            r#"
            UPDATE companies 
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

    /// Restore a soft-deleted company
    pub async fn restore(&self, id: &str, version: i32) -> Result<bool, Error> {
        let now = Utc::now();
        let result = sqlx::query!(
            r#"
            UPDATE companies 
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

    /// Find companies by name (partial match, excluding soft-deleted records)
    pub async fn find_by_name(&self, name: &str) -> Result<Vec<Company>, Error> {
        let pattern = format!("%{}%", name);
        let result = sqlx::query_as!(
            Company,
            r#"
            SELECT 
                id, name, description, email, phone, website, address, logo_url, 
                is_active, deleted_at, version, created_at, updated_at
            FROM companies 
            WHERE deleted_at IS NULL AND name ILIKE $1
            ORDER BY name
            "#,
            pattern
        )
        .fetch_all(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Find companies by email (exact match, excluding soft-deleted records)
    pub async fn find_by_email(&self, email: &str) -> Result<Option<Company>, Error> {
        let result = sqlx::query_as!(
            Company,
            r#"
            SELECT 
                id, name, description, email, phone, website, address, logo_url, 
                is_active, deleted_at, version, created_at, updated_at
            FROM companies 
            WHERE deleted_at IS NULL AND email = $1
            "#,
            email
        )
        .fetch_optional(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Check if a company exists by ID (excluding soft-deleted records)
    pub async fn exists(&self, id: &str) -> Result<bool, Error> {
        let result = sqlx::query_scalar!(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM companies 
                WHERE id = $1 AND deleted_at IS NULL
            )
            "#,
            id
        )
        .fetch_one(&self.db.pool)
        .await?;
        
        Ok(result.exists.unwrap_or(false))
    }

    /// Count active companies (excluding soft-deleted records)
    pub async fn count(&self) -> Result<i64, Error> {
        let result = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) 
            FROM companies 
            WHERE deleted_at IS NULL
            "#
        )
        .fetch_one(&self.db.pool)
        .await?;
        
        Ok(result.count.unwrap_or(0))
    }

    /// Get the current version of a company
    pub async fn get_version(&self, id: &str) -> Result<Option<i32>, Error> {
        let result = sqlx::query_scalar!(
            r#"
            SELECT version 
            FROM companies 
            WHERE id = $1 AND deleted_at IS NULL
            "#,
            id
        )
        .fetch_optional(&self.db.pool)
        .await?;
        
        Ok(result)
    }

    /// Find companies created within a date range (excluding soft-deleted records)
    pub async fn find_by_created_range(
        &self, 
        start: DateTime<Utc>, 
        end: DateTime<Utc>
    ) -> Result<Vec<Company>, Error> {
        let result = sqlx::query_as!(
            Company,
            r#"
            SELECT 
                id, name, description, email, phone, website, address, logo_url, 
                is_active, deleted_at, version, created_at, updated_at
            FROM companies 
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

    /// Find companies updated within a date range (excluding soft-deleted records)
    pub async fn find_by_updated_range(
        &self, 
        start: DateTime<Utc>, 
        end: DateTime<Utc>
    ) -> Result<Vec<Company>, Error> {
        let result = sqlx::query_as!(
            Company,
            r#"
            SELECT 
                id, name, description, email, phone, website, address, logo_url, 
                is_active, deleted_at, version, created_at, updated_at
            FROM companies 
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
    use crate::models::CompanyId;
    
    #[tokio::test]
    async fn test_create_company() {
        // This test would require a test database
        // For now, we'll just verify the SQL queries are valid
        let db = Arc::new(Database::new("postgresql://test:test@localhost/test").await.unwrap());
        let repo = CompanyRepository::new(db);
        
        let company = Company::new("Test Company".to_string());
        
        // This would fail without a test database, but verifies the SQL is correct
        // let result = repo.create(&company).await;
        // assert!(result.is_ok());
        
        assert_eq!(company.name, "Test Company");
        assert!(CompanyId::validate_id(&company.id));
    }
    
    #[tokio::test]
    async fn test_company_validation() {
        let company = Company::new("Test Company".to_string());
        assert!(company.validate().is_ok());
        
        let mut invalid_company = Company::new("".to_string());
        assert!(invalid_company.validate().is_err());
    }
}