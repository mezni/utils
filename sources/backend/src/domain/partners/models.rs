use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct PartnerProfile {
    pub id: String,
    pub user_id: String,
    pub classification: String,
    pub display_name: String,
    pub is_test: bool,
}
