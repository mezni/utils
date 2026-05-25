use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct Station {
    pub id: String,
    pub owner_id: String,
    pub name: String,
    pub city: String,
    pub is_operational: bool,
    pub is_test: bool,
}
