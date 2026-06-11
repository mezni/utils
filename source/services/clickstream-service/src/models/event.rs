use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::errors::AppError;

const MVP_TAXONOMY: &[&str] = &[
    "map_open",
    "station_view",
    "station_click",
    "nearby_search",
    "map_pan",
    "map_zoom",
];

const MAX_EVENT_SIZE: usize = 64 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    #[serde(default)]
    pub event_name: String,
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub session_id: String,
    pub client_ts: Option<DateTime<Utc>>,
    #[serde(default)]
    pub payload: Option<Value>,
}

impl Event {
    pub fn validate(&self) -> Result<(), AppError> {
        if self.event_name.is_empty() {
            return Err(AppError::invalid_event_name(
                "",
                MVP_TAXONOMY,
            ));
        }
        if !MVP_TAXONOMY.contains(&self.event_name.as_str()) {
            return Err(AppError::invalid_event_name(
                &self.event_name,
                MVP_TAXONOMY,
            ));
        }
        if self.session_id.is_empty() {
            return Err(AppError::missing_session_id());
        }
        if self.client_ts.is_none() {
            return Err(AppError::invalid_timestamp());
        }
        if let Some(ref p) = self.payload {
            if !p.is_object() {
                return Err(AppError::invalid_payload());
            }
        }
        let size = serde_json::to_vec(self)
            .map(|v| v.len())
            .unwrap_or(0);
        if size > MAX_EVENT_SIZE {
            return Err(AppError::payload_too_large());
        }
        Ok(())
    }
}
