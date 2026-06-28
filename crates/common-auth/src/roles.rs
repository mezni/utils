use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Driver,
    Partner,
    Admin,
}

impl Role {
    pub fn can_manage_stations(&self) -> bool {
        matches!(self, Role::Partner | Role::Admin)
    }

    pub fn can_manage_partners(&self) -> bool {
        matches!(self, Role::Admin)
    }

    pub fn can_manage_users(&self) -> bool {
        matches!(self, Role::Admin)
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Driver => write!(f, "driver"),
            Role::Partner => write!(f, "partner"),
            Role::Admin => write!(f, "admin"),
        }
    }
}

impl TryFrom<&str> for Role {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "driver" => Ok(Role::Driver),
            "partner" => Ok(Role::Partner),
            "admin" => Ok(Role::Admin),
            _ => Err(format!("Invalid role: {s}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_role_permissions() {
        assert!(!Role::Driver.can_manage_stations());
        assert!(Role::Partner.can_manage_stations());
        assert!(Role::Admin.can_manage_stations());

        assert!(!Role::Driver.can_manage_partners());
        assert!(!Role::Partner.can_manage_partners());
        assert!(Role::Admin.can_manage_partners());
    }

    #[test]
    fn test_role_from_str() {
        assert_eq!(Role::try_from("admin").unwrap(), Role::Admin);
        assert_eq!(Role::try_from("partner").unwrap(), Role::Partner);
        assert_eq!(Role::try_from("driver").unwrap(), Role::Driver);
        assert!(Role::try_from("unknown").is_err());
    }
}
