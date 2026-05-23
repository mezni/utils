use crate::models::{BaseModel, FullModel, CompanyId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, Validate)]
pub struct Company {
    #[serde(flatten)]
    pub base: BaseModel,
    pub name: String,
    pub description: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub website: Option<String>,
    pub address: Option<String>,
    pub logo_url: Option<String>,
    pub is_active: bool,
    #[serde(skip)]
    pub deleted_at: Option<DateTime<Utc>>,
    pub version: i32,
}

impl Company {
    /// Create a new company with default values
    pub fn new(name: String) -> Self {
        let now = Utc::now();
        Self {
            base: BaseModel {
                id: CompanyId::generate_id(),
                created_at: now,
                updated_at: now,
            },
            name,
            description: None,
            email: None,
            phone: None,
            website: None,
            address: None,
            logo_url: None,
            is_active: true,
            deleted_at: None,
            version: 1,
        }
    }

    /// Create a company with all fields
    pub fn create(
        name: String,
        description: Option<String>,
        email: Option<String>,
        phone: Option<String>,
        website: Option<String>,
        address: Option<String>,
        logo_url: Option<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            base: BaseModel {
                id: CompanyId::generate_id(),
                created_at: now,
                updated_at: now,
            },
            name,
            description,
            email,
            phone,
            website,
            address,
            logo_url,
            is_active: true,
            deleted_at: None,
            version: 1,
        }
    }

    /// Update company information
    pub fn update(
        &mut self,
        name: Option<String>,
        description: Option<String>,
        email: Option<String>,
        phone: Option<String>,
        website: Option<String>,
        address: Option<String>,
        logo_url: Option<String>,
        is_active: Option<bool>,
    ) {
        let now = Utc::now();
        
        if let Some(name) = name {
            self.name = name;
        }
        if let Some(description) = description {
            self.description = description;
        }
        if let Some(email) = email {
            self.email = email;
        }
        if let Some(phone) = phone {
            self.phone = phone;
        }
        if let Some(website) = website {
            self.website = website;
        }
        if let Some(address) = address {
            self.address = address;
        }
        if let Some(logo_url) = logo_url {
            self.logo_url = logo_url;
        }
        if let Some(is_active) = is_active {
            self.is_active = is_active;
        }
        
        self.base.updated_at = now;
        self.version += 1;
    }

    /// Soft delete the company
    pub fn delete(&mut self) {
        self.deleted_at = Some(Utc::now());
        self.base.updated_at = Utc::now();
        self.version += 1;
    }

    /// Restore the company (undo soft delete)
    pub fn restore(&mut self) {
        self.deleted_at = None;
        self.base.updated_at = Utc::now();
        self.version += 1;
    }

    /// Check if the company is deleted
    pub fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }

    /// Check if the company is active (not deleted and is_active flag is true)
    pub fn is_active(&self) -> bool {
        !self.is_deleted() && self.is_active
    }

    /// Get the company ID
    pub fn id(&self) -> &str {
        &self.base.id
    }

    /// Get the company name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the company version
    pub fn version(&self) -> i32 {
        self.version
    }

    /// Get the next version number
    pub fn next_version(&self) -> i32 {
        self.version + 1
    }

    /// Validate the company data
    pub fn validate(&self) -> Result<(), String> {
        // Validate name
        if self.name.trim().is_empty() {
            return Err("Company name cannot be empty".to_string());
        }
        if self.name.len() > 255 {
            return Err("Company name cannot exceed 255 characters".to_string());
        }

        // Validate email if provided
        if let Some(ref email) = self.email {
            if !email.trim().is_empty() {
                // Simple email validation
                if !email.contains('@') || !email.contains('.') {
                    return Err("Invalid email format".to_string());
                }
                if email.len() > 255 {
                    return Err("Email cannot exceed 255 characters".to_string());
                }
            }
        }

        // Validate phone if provided
        if let Some(ref phone) = self.phone {
            if !phone.trim().is_empty() {
                if phone.len() > 50 {
                    return Err("Phone number cannot exceed 50 characters".to_string());
                }
            }
        }

        // Validate website if provided
        if let Some(ref website) = self.website {
            if !website.trim().is_empty() {
                if !website.starts_with("http://") && !website.starts_with("https://") {
                    return Err("Website URL must start with http:// or https://".to_string());
                }
                if website.len() > 255 {
                    return Err("Website URL cannot exceed 255 characters".to_string());
                }
            }
        }

        // Validate version
        if self.version < 1 {
            return Err("Version must be greater than or equal to 1".to_string());
        }

        Ok(())
    }
}

impl From<Company> for FullModel {
    fn from(company: Company) -> Self {
        FullModel {
            base: company.base,
            deleted_at: company.deleted_at,
            version: company.version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_new_company() {
        let company = Company::new("Test Company".to_string());
        
        assert_eq!(company.name, "Test Company");
        assert!(company.description.is_none());
        assert!(company.email.is_none());
        assert!(company.is_active);
        assert!(!company.is_deleted());
        assert_eq!(company.version, 1);
        assert!(CompanyId::validate_id(&company.id));
    }

    #[test]
    fn test_create_company_with_all_fields() {
        let company = Company::create(
            "Test Company".to_string(),
            Some("Test Description".to_string()),
            Some("test@example.com".to_string()),
            Some("+216-71-123-456".to_string()),
            Some("https://example.com".to_string()),
            Some("Test Address".to_string()),
            Some("https://example.com/logo.png".to_string()),
        );
        
        assert_eq!(company.name, "Test Company");
        assert_eq!(company.description, Some("Test Description".to_string()));
        assert_eq!(company.email, Some("test@example.com".to_string()));
        assert_eq!(company.phone, Some("+216-71-123-456".to_string()));
        assert_eq!(company.website, Some("https://example.com".to_string()));
        assert_eq!(company.address, Some("Test Address".to_string()));
        assert_eq!(company.logo_url, Some("https://example.com/logo.png".to_string()));
    }

    #[test]
    fn test_update_company() {
        let mut company = Company::new("Test Company".to_string());
        let original_version = company.version;
        
        company.update(
            Some("Updated Company".to_string()),
            Some("Updated Description".to_string()),
            Some("updated@example.com".to_string()),
            Some("+216-71-654-321".to_string()),
            Some("https://updated.com".to_string()),
            Some("Updated Address".to_string()),
            Some("https://updated.com/logo.png".to_string()),
            Some(false),
        );
        
        assert_eq!(company.name, "Updated Company");
        assert_eq!(company.description, Some("Updated Description".to_string()));
        assert_eq!(company.email, Some("updated@example.com".to_string()));
        assert_eq!(company.phone, Some("+216-71-654-321".to_string()));
        assert_eq!(company.website, Some("https://updated.com".to_string()));
        assert_eq!(company.address, Some("Updated Address".to_string()));
        assert_eq!(company.logo_url, Some("https://updated.com/logo.png".to_string()));
        assert!(!company.is_active);
        assert_eq!(company.version, original_version + 1);
    }

    #[test]
    fn test_soft_delete_company() {
        let mut company = Company::new("Test Company".to_string());
        
        assert!(!company.is_deleted());
        assert!(company.is_active());
        
        company.delete();
        
        assert!(company.is_deleted());
        assert!(!company.is_active());
        assert!(company.deleted_at.is_some());
    }

    #[test]
    fn test_restore_company() {
        let mut company = Company::new("Test Company".to_string());
        company.delete();
        
        assert!(company.is_deleted());
        
        company.restore();
        
        assert!(!company.is_deleted());
        assert!(company.is_active());
        assert!(company.deleted_at.is_none());
    }

    #[test]
    fn test_validate_company() {
        let mut company = Company::new("Test Company".to_string());
        
        // Valid company should pass validation
        assert!(company.validate().is_ok());
        
        // Empty name should fail
        company.name = "".to_string();
        assert!(company.validate().is_err());
        
        // Name too long should fail
        company.name = "a".repeat(256);
        assert!(company.validate().is_err());
        
        // Invalid email should fail
        company.name = "Test Company".to_string();
        company.email = Some("invalid-email".to_string());
        assert!(company.validate().is_err());
        
        // Email too long should fail
        company.email = Some(format!("{}@example.com", "a".repeat(245)));
        assert!(company.validate().is_err());
        
        // Invalid website should fail
        company.email = None;
        company.website = Some("invalid-website".to_string());
        assert!(company.validate().is_err());
        
        // Website too long should fail
        company.website = Some(format!("https://{}", "a".repeat(240)));
        assert!(company.validate().is_err());
        
        // Invalid version should fail
        company.website = None;
        company.version = 0;
        assert!(company.validate().is_err());
    }
}