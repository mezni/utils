use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginationMeta {
    pub page: i32,
    pub size: i32,
    pub total: i32,
    pub total_pages: i32,
    pub has_next: bool,
    pub has_prev: bool,
}

/// Always-success API response. `success` must always be `true` — construct
/// via [`SuccessEnvelope::new`] to guarantee this invariant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuccessEnvelope<T = serde_json::Value> {
    pub success: bool,
    pub data: T,
    pub meta: PaginationMeta,
}

impl<T> SuccessEnvelope<T> {
    pub fn new(data: T, meta: PaginationMeta) -> Self {
        SuccessEnvelope {
            success: true,
            data,
            meta,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorDetail {
    pub code: String,
    pub message: String,
    pub details: Option<serde_json::Value>,
}

/// Always-error API response. `success` must always be `false`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorEnvelope {
    pub success: bool,
    pub error: ErrorDetail,
}
