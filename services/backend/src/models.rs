use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Partner {
    pub id: Uuid,
    pub name: String,
    pub email: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Station {
    pub id: Uuid,
    pub partner_id: Uuid,
    pub name: String,
    pub address: Option<String>,
    pub is_active: bool,
    pub latitude: f64,
    pub longitude: f64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Connector {
    pub id: Uuid,
    pub station_id: Uuid,
    pub connector_type: String,
    pub max_power_kw: Option<sqlx::types::BigDecimal>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Driver {
    pub id: Uuid,
    pub email: String,
    pub display_name: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Invitation {
    pub id: Uuid,
    pub token: Uuid,
    pub email: String,
    pub partner_id: Option<Uuid>,
    pub expires_at: DateTime<Utc>,
    pub used_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct AppConfig {
    pub id: Uuid,
    pub key: String,
    pub value: serde_json::Value,
    pub updated_at: DateTime<Utc>,
}

// Request/Response DTOs

#[derive(Debug, Deserialize)]
pub struct StationQuery {
    pub lat: f64,
    pub lng: f64,
    pub radius: f64,
}

#[derive(Debug, Serialize)]
pub struct StationResponse {
    pub id: Uuid,
    pub name: String,
    pub address: Option<String>,
    pub latitude: f64,
    pub longitude: f64,
    pub is_active: bool,
    pub connectors: Vec<ConnectorSummary>,
}

#[derive(Debug, Serialize)]
pub struct ConnectorSummary {
    pub id: Uuid,
    pub connector_type: String,
    pub status: String,
}

#[derive(Debug, Deserialize)]
pub struct TelemetryBatch {
    pub events: Vec<TelemetryEvent>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TelemetryEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub timestamp: DateTime<Utc>,
    pub screen: Option<String>,
    pub payload: Option<serde_json::Value>,
}
