use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Status {
    ACTIVE,
    INACTIVE,
    MAINTENANCE,
    DISABLED,
}

impl Status {
    pub fn as_str(&self) -> &'static str {
        match self {
            Status::ACTIVE => "ACTIVE",
            Status::INACTIVE => "INACTIVE",
            Status::MAINTENANCE => "MAINTENANCE",
            Status::DISABLED => "DISABLED",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "ACTIVE" => Some(Status::ACTIVE),
            "INACTIVE" => Some(Status::INACTIVE),
            "MAINTENANCE" => Some(Status::MAINTENANCE),
            "DISABLED" => Some(Status::DISABLED),
            _ => None,
        }
    }
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
