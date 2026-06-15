use serde::{Deserialize, Serialize};

pub const MVP1_FALLBACK_OPERATOR: &str = "usr-mvp1-fallback";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimsContext {
    pub user_id: String,
    pub active_role: String,
}

impl ClaimsContext {
    pub fn mock_mvp1_context() -> Self {
        Self {
            user_id: MVP1_FALLBACK_OPERATOR.to_string(),
            active_role: "SYSTEM_ADMIN".to_string(),
        }
    }
}
