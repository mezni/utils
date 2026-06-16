use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct DriverProfile {
    pub id: String,
    pub keycloak_id: Uuid,
    pub display_name: Option<String>,
    pub email: String,
    pub created_at: DateTime<Utc>,
}
