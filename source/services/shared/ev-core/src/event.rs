use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyticsEvent {
    pub id: Option<i64>,
    pub event_type: String,
    pub session_id: String,
    pub user_id: Option<String>,
    pub payload: Option<serde_json::Value>,
    pub occurred_at: chrono::NaiveDateTime,
    pub ingested_at: Option<chrono::NaiveDateTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateEventRequest {
    pub event_type: String,
    pub session_id: String,
    pub user_id: Option<String>,
    pub occurred_at: chrono::NaiveDateTime,
    pub payload: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateBatchEventsRequest {
    pub events: Vec<CreateEventRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchEventResponse {
    pub ingested: usize,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventResponse {
    pub id: i64,
    pub event_type: String,
    pub session_id: String,
    pub occurred_at: chrono::NaiveDateTime,
    pub ingested_at: chrono::NaiveDateTime,
}
