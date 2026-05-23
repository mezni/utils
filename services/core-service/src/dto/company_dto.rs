use serde::{Deserialize, Serialize};
use validator::Validate;

#[derive(Debug, Serialize, Deserialize, Validate)]
pub struct CreateCompanyRequest {
    #[validate(length(min = 1, max = 255, message = "Company name must be between 1 and 255 characters"))]
    pub name: String,
    
    #[serde(default)]
    pub description: Option<String>,
    
    #[serde(default)]
    #[validate(email(message = "Invalid email format"))]
    pub email: Option<String>,
    
    #[serde(default)]
    pub phone: Option<String>,
    
    #[serde(default)]
    pub website: Option<String>,
    
    #[serde(default)]
    pub address: Option<String>,
    
    #[serde(default)]
    pub logo_url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Validate)]
pub struct UpdateCompanyRequest {
    #[validate(length(min = 1, max = 255, message = "Company name must be between 1 and 255 characters"))]
    pub name: Option<String>,
    
    #[serde(default)]
    pub description: Option<String>,
    
    #[serde(default)]
    #[validate(email(message = "Invalid email format"))]
    pub email: Option<String>,
    
    #[serde(default)]
    pub phone: Option<String>,
    
    #[serde(default)]
    pub website: Option<String>,
    
    #[serde(default)]
    pub address: Option<String>,
    
    #[serde(default)]
    pub logo_url: Option<String>,
    
    #[serde(default)]
    pub is_active: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompanyResponse {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub website: Option<String>,
    pub address: Option<String>,
    pub logo_url: Option<String>,
    pub is_active: bool,
    pub version: i32,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<crate::models::Company> for CompanyResponse {
    fn from(company: crate::models::Company) -> Self {
        Self {
            id: company.base.id,
            name: company.name,
            description: company.description,
            email: company.email,
            phone: company.phone,
            website: company.website,
            address: company.address,
            logo_url: company.logo_url,
            is_active: company.is_active,
            version: company.version,
            created_at: company.base.created_at,
            updated_at: company.base.updated_at,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompanyListResponse {
    pub companies: Vec<CompanyResponse>,
    pub total: usize,
}