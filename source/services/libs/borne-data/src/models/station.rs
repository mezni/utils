use chrono::{DateTime, Utc};
use sqlx::FromRow;

#[derive(Debug, Clone, FromRow)]
pub struct Station {
    pub id: String,
    pub partner_id: String,
    pub name: String,
    pub address: Option<String>,
    pub latitude: f64,
    pub longitude: f64,
    pub created_at: DateTime<Utc>,
    pub created_by: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub updated_by: Option<String>,
}
