use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectorType {
    pub id: String,
    pub name: String,
    pub description: String,
}
