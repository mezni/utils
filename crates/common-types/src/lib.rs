pub mod api;
pub mod events;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum EntityPrefix {
    #[serde(rename = "USR")]
    Usr,
    #[serde(rename = "PRT")]
    Prt,
    #[serde(rename = "STN")]
    Stn,
    #[serde(rename = "CHG")]
    Chg,
    #[serde(rename = "REV")]
    Rev,
    #[serde(rename = "EVT")]
    Evt,
    #[serde(rename = "CLK")]
    Clk,
    #[serde(rename = "SESS")]
    Sess,
    #[serde(rename = "ANON")]
    Anon,
}

impl EntityPrefix {
    pub fn as_str(&self) -> &'static str {
        match self {
            EntityPrefix::Usr => "USR",
            EntityPrefix::Prt => "PRT",
            EntityPrefix::Stn => "STN",
            EntityPrefix::Chg => "CHG",
            EntityPrefix::Rev => "REV",
            EntityPrefix::Evt => "EVT",
            EntityPrefix::Clk => "CLK",
            EntityPrefix::Sess => "SESS",
            EntityPrefix::Anon => "ANON",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum Role {
    #[serde(rename = "registered_driver")]
    RegisteredDriver,
    #[serde(rename = "partner")]
    Partner,
    #[serde(rename = "admin")]
    Admin,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum StationStatus {
    #[serde(rename = "active")]
    Active,
    #[serde(rename = "inactive")]
    Inactive,
    #[serde(rename = "maintenance")]
    Maintenance,
    #[serde(rename = "draft")]
    Draft,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum StationAvailabilityStatus {
    #[serde(rename = "available")]
    Available,
    #[serde(rename = "limited")]
    Limited,
    #[serde(rename = "unavailable")]
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum PartnerStatus {
    #[serde(rename = "active")]
    Active,
    #[serde(rename = "suspended")]
    Suspended,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ChargerStatus {
    #[serde(rename = "available")]
    Available,
    #[serde(rename = "offline")]
    Offline,
    #[serde(rename = "fault")]
    Fault,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ChargerType {
    #[serde(rename = "CCS")]
    Ccs,
    #[serde(rename = "Type2")]
    Type2,
    #[serde(rename = "CHAdeMO")]
    Chademo,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ReviewStatus {
    #[serde(rename = "published")]
    Published,
    #[serde(rename = "hidden")]
    Hidden,
    #[serde(rename = "flagged")]
    Flagged,
    #[serde(rename = "deleted")]
    Deleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum PartnerRole {
    #[serde(rename = "owner")]
    Owner,
    #[serde(rename = "manager")]
    Manager,
    #[serde(rename = "operator")]
    Operator,
    #[serde(rename = "viewer")]
    Viewer,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum GisQueueStatus {
    #[serde(rename = "pending")]
    Pending,
    #[serde(rename = "processing")]
    Processing,
    #[serde(rename = "done")]
    Done,
    #[serde(rename = "failed")]
    Failed,
    #[serde(rename = "dead_letter")]
    DeadLetter,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum AvailabilitySource {
    #[serde(rename = "manual_partner")]
    ManualPartner,
    #[serde(rename = "system_sync")]
    SystemSync,
    #[serde(rename = "admin")]
    Admin,
}
