use serde::{Deserialize, Serialize};

/// Payload for FAVORITE_ADDED and FAVORITE_REMOVED events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FavoriteEventPayload {
    pub station_id: String,
}

/// Payload for SEARCH_EXECUTED event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchExecutedPayload {
    pub query_text: String,
    pub result_count: usize,
}

/// Payload for SEARCH_SELECTED event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchSelectedPayload {
    pub query_text: String,
    pub station_id: String,
    pub position: usize,
}

/// Payload for FILTER_CHANGED event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterChangedPayload {
    pub filter_type: String,
    pub old_value: Option<String>,
    pub new_value: Option<String>,
}

/// Payload for OFFLINE_MODE_ENTERED event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OfflineModeEnteredPayload {
    pub duration_seconds: Option<u64>,
}

/// Helper to construct telemetry event payloads
pub fn favorite_added_payload(station_id: &str) -> serde_json::Value {
    serde_json::json!({
        "station_id": station_id,
    })
}

pub fn favorite_removed_payload(station_id: &str) -> serde_json::Value {
    serde_json::json!({
        "station_id": station_id,
    })
}

pub fn search_executed_payload(query: &str, result_count: usize) -> serde_json::Value {
    serde_json::json!({
        "query_text": query,
        "result_count": result_count,
    })
}

pub fn search_selected_payload(query: &str, station_id: &str, position: usize) -> serde_json::Value {
    serde_json::json!({
        "query_text": query,
        "station_id": station_id,
        "position": position,
    })
}

pub fn filter_changed_payload(
    filter_type: &str,
    old_value: Option<&str>,
    new_value: Option<&str>,
) -> serde_json::Value {
    serde_json::json!({
        "filter_type": filter_type,
        "old_value": old_value,
        "new_value": new_value,
    })
}

pub fn offline_mode_payload(duration_seconds: Option<u64>) -> serde_json::Value {
    serde_json::json!({
        "duration_seconds": duration_seconds,
    })
}
