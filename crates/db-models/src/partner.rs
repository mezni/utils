use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct Partner {
    pub id: String,
    pub name: String,
    #[sqlx(rename = "type")]
    pub partner_type: String,
    pub email: String,
    pub phone: Option<String>,
    pub address: Option<String>,
    pub website: Option<String>,
    pub status: String,
    pub keycloak_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}
