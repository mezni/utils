use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserProfile {
    pub user_uuid: Uuid,
    pub email: String,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub phone: Option<String>,
    pub locale: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateProfileRequest {
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub phone: Option<String>,
    pub locale: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_request_all_fields() {
        let req = UpdateProfileRequest {
            first_name: Some("John".into()),
            last_name: Some("Doe".into()),
            phone: Some("+21612345678".into()),
            locale: Some("fr-TN".into()),
        };
        assert_eq!(req.first_name, Some("John".into()));
        assert_eq!(req.last_name, Some("Doe".into()));
        assert_eq!(req.phone, Some("+21612345678".into()));
        assert_eq!(req.locale, Some("fr-TN".into()));
    }

    #[test]
    fn test_update_request_partial_fields() {
        let req = UpdateProfileRequest {
            first_name: Some("Jane".into()),
            last_name: None,
            phone: None,
            locale: None,
        };
        assert_eq!(req.first_name, Some("Jane".into()));
        assert_eq!(req.last_name, None);
    }

    #[test]
    fn test_update_request_empty_fields() {
        let req = UpdateProfileRequest {
            first_name: None,
            last_name: None,
            phone: None,
            locale: None,
        };
        assert!(req.first_name.is_none());
        assert!(req.last_name.is_none());
    }
}
