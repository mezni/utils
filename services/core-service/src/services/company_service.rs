use crate::models::Company;
use crate::repositories::CompanyRepository;
use crate::utils::database::Database;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CompanyServiceError {
    #[error("Company not found: {0}")]
    NotFound(String),
    #[error("Validation error: {0}")]
    Validation(String),
    #[error("Optimistic lock error: {0}")]
    OptimisticLock(String),
    #[error("Database error: {0}")]
    Database(String),
    #[error("Company already exists with email: {0}")]
    EmailAlreadyExists(String),
    #[error("Company is soft-deleted: {0}")]
    SoftDeleted(String),
}

impl From<sqlx::Error> for CompanyServiceError {
    fn from(err: sqlx::Error) -> Self {
        CompanyServiceError::Database(err.to_string())
    }
}

pub struct CompanyService {
    repository: CompanyRepository,
}

impl CompanyService {
    /// Create a new CompanyService
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            repository: CompanyRepository::new(db),
        }
    }

    /// Create a new company
    pub async fn create_company(
        &self,
        name: String,
        description: Option<String>,
        email: Option<String>,
        phone: Option<String>,
        website: Option<String>,
        address: Option<String>,
        logo_url: Option<String>,
    ) -> Result<Company, CompanyServiceError> {
        // Validate input data
        if name.trim().is_empty() {
            return Err(CompanyServiceError::Validation("Company name cannot be empty".to_string()));
        }

        if name.len() > 255 {
            return Err(CompanyServiceError::Validation("Company name cannot exceed 255 characters".to_string()));
        }

        // Check if email already exists
        if let Some(ref email) = email {
            if !email.trim().is_empty() {
                if let Some(existing) = self.repository.find_by_email(email).await? {
                    if !existing.is_deleted() {
                        return Err(CompanyServiceError::EmailAlreadyExists(email.clone()));
                    }
                }
            }
        }

        // Create company
        let mut company = Company::create(name, description, email, phone, website, address, logo_url);
        
        // Validate company data
        if let Err(err) = company.validate() {
            return Err(CompanyServiceError::Validation(err));
        }

        // Save to database
        let saved_company = self.repository.create(&company).await?;
        Ok(saved_company)
    }

    /// Get a company by ID
    pub async fn get_company(&self, id: &str) -> Result<Company, CompanyServiceError> {
        // Validate company ID format
        if !crate::models::CompanyId::validate_id(id) {
            return Err(CompanyServiceError::Validation("Invalid company ID format".to_string()));
        }

        let company = self.repository.find_by_id(id).await?
            .ok_or_else(|| CompanyServiceError::NotFound(id.to_string()))?;

        Ok(company)
    }

    /// Get a company by ID (including soft-deleted records)
    pub async fn get_company_including_deleted(&self, id: &str) -> Result<Company, CompanyServiceError> {
        // Validate company ID format
        if !crate::models::CompanyId::validate_id(id) {
            return Err(CompanyServiceError::Validation("Invalid company ID format".to_string()));
        }

        let company = self.repository.find_by_id_including_deleted(id).await?
            .ok_or_else(|| CompanyServiceError::NotFound(id.to_string()))?;

        Ok(company)
    }

    /// Get all active companies
    pub async fn get_all_companies(&self) -> Result<Vec<Company>, CompanyServiceError> {
        let companies = self.repository.find_all().await?;
        Ok(companies)
    }

    /// Update a company
    pub async fn update_company(
        &self,
        id: &str,
        name: Option<String>,
        description: Option<String>,
        email: Option<String>,
        phone: Option<String>,
        website: Option<String>,
        address: Option<String>,
        logo_url: Option<String>,
        is_active: Option<bool>,
    ) -> Result<Company, CompanyServiceError> {
        // Validate company ID format
        if !crate::models::CompanyId::validate_id(id) {
            return Err(CompanyServiceError::Validation("Invalid company ID format".to_string()));
        }

        // Get current company
        let mut company = self.get_company(id).await?;

        // Check if email already exists (if changing email)
        if let Some(ref new_email) = email {
            if !new_email.trim().is_empty() && new_email != &company.email.clone().unwrap_or_default() {
                if let Some(existing) = self.repository.find_by_email(new_email).await? {
                    if existing.id != company.id && !existing.is_deleted() {
                        return Err(CompanyServiceError::EmailAlreadyExists(new_email.clone()));
                    }
                }
            }
        }

        // Update company data
        company.update(name, description, email, phone, website, address, logo_url, is_active);

        // Validate updated company
        if let Err(err) = company.validate() {
            return Err(CompanyServiceError::Validation(err));
        }

        // Save to database
        match self.repository.update(&company).await {
            Ok(updated_company) => Ok(updated_company),
            Err(_) => Err(CompanyServiceError::OptimisticLock(
                "Company was modified by another transaction".to_string()
            )),
        }
    }

    /// Soft delete a company
    pub async fn delete_company(&self, id: &str) -> Result<bool, CompanyServiceError> {
        // Validate company ID format
        if !crate::models::CompanyId::validate_id(id) {
            return Err(CompanyServiceError::Validation("Invalid company ID format".to_string()));
        }

        // Get current company to check version
        let company = self.get_company(id).await?;

        // Delete company
        match self.repository.delete(id, company.version).await {
            Ok(success) => {
                if success {
                    Ok(true)
                } else {
                    Err(CompanyServiceError::OptimisticLock(
                        "Company was modified by another transaction".to_string()
                    ))
                }
            }
            Err(_) => Err(CompanyServiceError::OptimisticLock(
                "Company was modified by another transaction".to_string()
            )),
        }
    }

    /// Restore a soft-deleted company
    pub async fn restore_company(&self, id: &str) -> Result<bool, CompanyServiceError> {
        // Validate company ID format
        if !crate::models::CompanyId::validate_id(id) {
            return Err(CompanyServiceError::Validation("Invalid company ID format".to_string()));
        }

        // Get current company (including deleted) to check version
        let company = self.get_company_including_deleted(id).await?;

        if !company.is_deleted() {
            return Err(CompanyServiceError::Validation("Company is not deleted".to_string()));
        }

        // Restore company
        match self.repository.restore(id, company.version).await {
            Ok(success) => {
                if success {
                    Ok(true)
                } else {
                    Err(CompanyServiceError::OptimisticLock(
                        "Company was modified by another transaction".to_string()
                    ))
                }
            }
            Err(_) => Err(CompanyServiceError::OptimisticLock(
                "Company was modified by another transaction".to_string()
            )),
        }
    }

    /// Search companies by name
    pub async fn search_companies_by_name(&self, name: &str) -> Result<Vec<Company>, CompanyServiceError> {
        if name.trim().is_empty() {
            return Err(CompanyServiceError::Validation("Search term cannot be empty".to_string()));
        }

        if name.len() > 255 {
            return Err(CompanyServiceError::Validation("Search term cannot exceed 255 characters".to_string()));
        }

        let companies = self.repository.find_by_name(name).await?;
        Ok(companies)
    }

    /// Find companies created within a date range
    pub async fn find_companies_created_between(
        &self,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<Company>, CompanyServiceError> {
        if start > end {
            return Err(CompanyServiceError::Validation("Start date must be before end date".to_string()));
        }

        let companies = self.repository.find_by_created_range(start, end).await?;
        Ok(companies)
    }

    /// Find companies updated within a date range
    pub async fn find_companies_updated_between(
        &self,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<Company>, CompanyServiceError> {
        if start > end {
            return Err(CompanyServiceError::Validation("Start date must be before end date".to_string()));
        }

        let companies = self.repository.find_by_updated_range(start, end).await?;
        Ok(companies)
    }

    /// Check if a company exists
    pub async fn company_exists(&self, id: &str) -> Result<bool, CompanyServiceError> {
        // Validate company ID format
        if !crate::models::CompanyId::validate_id(id) {
            return Err(CompanyServiceError::Validation("Invalid company ID format".to_string()));
        }

        let exists = self.repository.exists(id).await?;
        Ok(exists)
    }

    /// Get company count
    pub async fn get_company_count(&self) -> Result<i64, CompanyServiceError> {
        let count = self.repository.count().await?;
        Ok(count)
    }

    /// Get company version
    pub async fn get_company_version(&self, id: &str) -> Result<Option<i32>, CompanyServiceError> {
        // Validate company ID format
        if !crate::models::CompanyId::validate_id(id) {
            return Err(CompanyServiceError::Validation("Invalid company ID format".to_string()));
        }

        let version = self.repository.get_version(id).await?;
        Ok(version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::CompanyId;
    
    #[tokio::test]
    async fn test_create_company_validation() {
        // This test would require a test database
        let db = Arc::new(Database::new("postgresql://test:test@localhost/test").await.unwrap());
        let service = CompanyService::new(db);
        
        // Test empty name
        let result = service.create_company(
            "".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
        ).await;
        assert!(matches!(result, Err(CompanyServiceError::Validation(_))));
        
        // Test name too long
        let result = service.create_company(
            "a".repeat(256),
            None,
            None,
            None,
            None,
            None,
            None,
        ).await;
        assert!(matches!(result, Err(CompanyServiceError::Validation(_))));
        
        // Test invalid company ID
        let result = service.get_company("invalid-id").await;
        assert!(matches!(result, Err(CompanyServiceError::Validation(_))));
    }
}