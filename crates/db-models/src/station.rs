use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct Station {
    pub id: String,
    pub partner_id: String,
    pub name: String,
    pub address: String,
    pub city: String,
    pub postal_code: Option<String>,
    pub status: String,
    pub visibility: String,
    pub photo_url: Option<String>,
    pub description: Option<String>,
    pub access_notes: Option<String>,
    pub opening_hours: Option<String>,
    pub has_24h_access: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}
