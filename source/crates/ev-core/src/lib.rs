use thiserror::Error;

pub mod models;
pub mod types;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ConnectorType {
    CCS,
    CHAdeMO,
    Type2,
    Type3,
    Schuko,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum StationStatus {
    Available,
    Charging,
    Offline,
    Maintenance,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PartnerType {
    Operator,
    Owner,
    Roaming,
}

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Internal error: {0}")]
    Internal(String),
}
