use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct PartnerProfile {
    pub id: String,
    pub user_id: String,
    pub classification: String,
    pub display_name: String,
    pub tax_id: Option<String>,
    pub contact_phone: Option<String>,
    pub is_test: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
pub struct CreatePartnerRequest {
    pub user_id: String,
    pub classification: String,
    pub display_name: String,
    pub tax_id: Option<String>,
    pub contact_phone: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdatePartnerRequest {
    pub classification: Option<String>,
    pub display_name: Option<String>,
    pub tax_id: Option<String>,
    pub contact_phone: Option<String>,
    pub updated_at: DateTime<Utc>,
}
