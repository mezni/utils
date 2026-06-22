use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    Driver,
    Partner,
    Admin,
}

impl Role {
    pub fn precedence(&self) -> u8 {
        match self {
            Role::Driver => 10,
            Role::Partner => 20,
            Role::Admin => 30,
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "driver" => Some(Role::Driver),
            "partner" => Some(Role::Partner),
            "admin" => Some(Role::Admin),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Driver => "driver",
            Role::Partner => "partner",
            Role::Admin => "admin",
        }
    }

    pub fn inherits(&self, other: &Role) -> bool {
        self.precedence() >= other.precedence()
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Default for Role {
    fn default() -> Self {
        Role::Driver
    }
}
