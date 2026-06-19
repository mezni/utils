use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partner {
    pub id: String,
    pub name: String,
    pub network_type: NetworkType,
    pub support_phone: Option<String>,
    pub support_email: Option<String>,
    pub is_verified: bool,
    pub created_by: Option<String>,
    pub updated_by: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub deleted_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NetworkType {
    Individual,
    Company,
}

impl Partner {
    pub fn new(id: String, name: String, network_type: NetworkType) -> Self {
        Self {
            id,
            name,
            network_type,
            support_phone: None,
            support_email: None,
            is_verified: false,
            created_by: None,
            updated_by: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            deleted_at: None,
        }
    }

    pub fn update(&mut self, name: String, support_phone: Option<String>, support_email: Option<String>, is_verified: bool, updated_by: Option<String>) {
        self.name = name;
        self.support_phone = support_phone;
        self.support_email = support_email;
        self.is_verified = is_verified;
        self.updated_by = updated_by;
        self.updated_at = chrono::Utc::now();
    }

    pub fn soft_delete(&mut self, deleted_at: Option<chrono::DateTime<chrono::Utc>>) {
        self.deleted_at = deleted_at;
        self.updated_at = chrono::Utc::now();
    }

    pub fn is_active(&self) -> bool {
        self.deleted_at.is_none()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreatePartnerRequest {
    pub name: String,
    pub network_type: NetworkType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub support_phone: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub support_email: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdatePartnerRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub support_phone: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub support_email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_verified: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PartnerResponse {
    pub id: String,
    pub name: String,
    pub network_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub support_phone: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub support_email: Option<String>,
    pub is_verified: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<Partner> for PartnerResponse {
    fn from(partner: Partner) -> Self {
        Self {
            id: partner.id,
            name: partner.name,
            network_type: partner.network_type.to_string(),
            support_phone: partner.support_phone,
            support_email: partner.support_email,
            is_verified: partner.is_verified,
            created_by: partner.created_by,
            updated_by: partner.updated_by,
            created_at: partner.created_at.to_rfc3339(),
            updated_at: partner.updated_at.to_rfc3339(),
        }
    }
}
