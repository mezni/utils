use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct OffsetParams {
    pub limit: i64,
    pub offset: i64,
}

impl Default for OffsetParams {
    fn default() -> Self {
        Self {
            limit: 20,
            offset: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginatedResponse<T> {
    pub data: Vec<T>,
    pub total: i64,
    pub limit: i64,
    pub offset: i64,
}
