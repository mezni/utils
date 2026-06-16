use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct Charger {
    pub id: String,
    pub station_id: String,
    pub charger_type: String,
    pub connector: String,
    pub power_kw: rust_decimal::Decimal,
    pub identifier_code: Option<String>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}
