use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::role::Role;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserProfile {
    pub user_id: Uuid,
    pub email: String,
    pub role: Role,
    pub display_name: Option<String>,
    pub phone: Option<String>,
    pub locale: Option<String>,
    pub is_active: bool,
    pub last_login_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl UserProfile {
    pub fn new(user_id: Uuid, email: impl Into<String>, role: Role) -> Self {
        Self {
            user_id,
            email: email.into(),
            role,
            display_name: None,
            phone: None,
            locale: Some("en".to_string()),
            is_active: true,
            last_login_at: None,
        }
    }
}
