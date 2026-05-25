use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectorType {
    pub id: String,
    pub name: String,
    pub description: String,
}
