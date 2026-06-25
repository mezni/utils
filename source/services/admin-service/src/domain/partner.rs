use serde::{Deserialize, Serialize};
use super::nanoid::generate_nanoid;

const PREFIX: &str = "OPR";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partner {
    pub partner_id: String,
    pub name: String,
    pub partner_type: Option<String>,
    pub support_phone: Option<String>,
    pub support_email: Option<String>,
    pub is_verified: bool,
    pub created_by_uuid: Option<uuid::Uuid>,
    pub updated_by_uuid: Option<uuid::Uuid>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
    pub deleted_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl Partner {
    pub fn new(name: String, partner_type: Option<String>) -> Self {
        Self {
            partner_id: format!("{}-{}", PREFIX, generate_nanoid()),
            name,
            partner_type,
            support_phone: None,
            support_email: None,
            is_verified: false,
            created_by_uuid: None,
            updated_by_uuid: None,
            created_at: chrono::Utc::now(),
            updated_at: None,
            deleted_at: None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreatePartnerRequest {
    pub name: String,
    pub partner_type: Option<String>,
    pub support_phone: Option<String>,
    pub support_email: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdatePartnerRequest {
    pub name: Option<String>,
    pub partner_type: Option<String>,
    pub support_phone: Option<String>,
    pub support_email: Option<String>,
}

pub fn validate_partner_type(t: &str) -> bool {
    matches!(t, "INDIVIDUAL" | "COMPANY")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partner_id_format() {
        let p = Partner::new("Test".into(), Some("COMPANY".into()));
        assert!(p.partner_id.starts_with("OPR-"));
        assert_eq!(p.partner_id.len(), 16);
    }

    #[test]
    fn test_validate_partner_type() {
        assert!(validate_partner_type("INDIVIDUAL"));
        assert!(validate_partner_type("COMPANY"));
        assert!(!validate_partner_type("OTHER"));
        assert!(!validate_partner_type(""));
    }
}
