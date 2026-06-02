pub mod api;
pub mod events;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum EntityPrefix {
    Usr,
    Prt,
    Stn,
    Chg,
    Rev,
    Evt,
    Clk,
    Sess,
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
    RegisteredDriver,
    Partner,
    Admin,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum StationStatus {
    Active,
    Inactive,
    Maintenance,
    Draft,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum StationAvailabilityStatus {
    Available,
    Limited,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum PartnerStatus {
    Active,
    Suspended,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ChargerStatus {
    Available,
    Offline,
    Fault,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ChargerType {
    Ccs,
    Type2,
    Chademo,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ReviewStatus {
    Published,
    Hidden,
    Flagged,
    Deleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum PartnerRole {
    Owner,
    Manager,
    Operator,
    Viewer,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum GisQueueStatus {
    Pending,
    Processing,
    Done,
    Failed,
    DeadLetter,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum AvailabilitySource {
    ManualPartner,
    SystemSync,
    Admin,
}
