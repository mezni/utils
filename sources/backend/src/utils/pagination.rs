use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;

#[derive(Debug, Serialize, Deserialize)]
pub struct Cursor {
    pub created_at: DateTime<Utc>,
    pub id: String,
}

impl Cursor {
    pub fn encode(&self) -> String {
        let json = serde_json::to_vec(self).expect("cursor serialization is infallible");
        URL_SAFE_NO_PAD.encode(&json)
    }

    pub fn decode(encoded: &str) -> Result<Self, String> {
        let bytes = URL_SAFE_NO_PAD.decode(encoded).map_err(|e| e.to_string())?;
        serde_json::from_slice(&bytes).map_err(|e| e.to_string())
    }
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub cursor: Option<String>,
    pub limit: Option<i64>,
    pub include_test: Option<bool>,
}

impl ListQuery {
    pub fn limit(&self) -> i64 {
        self.limit.unwrap_or(50).clamp(1, 100)
    }

    pub fn include_test(&self) -> bool {
        self.include_test.unwrap_or(false)
    }
}

#[derive(Debug, Serialize)]
pub struct Pagination {
    pub next_cursor: Option<String>,
    pub has_more: bool,
}
