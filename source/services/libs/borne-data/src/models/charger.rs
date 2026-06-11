use chrono::{DateTime, Utc};
use sqlx::FromRow;

#[derive(Debug, Clone, FromRow)]
pub struct Charger {
    pub id: String,
    pub station_id: String,
    pub connector_type: String,
    pub power_kw: f64,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub created_by: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub updated_by: Option<String>,
}
