use chrono::{DateTime, Utc};
use sqlx::FromRow;

#[derive(Debug, Clone, FromRow)]
pub struct Partner {
    pub id: String,
    pub name: String,
    pub r#type: String,
    pub is_verified: bool,
    pub is_active: bool,
    pub is_live: bool,
    pub created_at: DateTime<Utc>,
    pub created_by: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub updated_by: Option<String>,
}
