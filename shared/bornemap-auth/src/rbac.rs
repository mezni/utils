use bornemap_core::AppError;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    Admin,
    Partner,
    RegisteredDriver,
}

impl Role {
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Admin => "ADMIN",
            Role::Partner => "PARTNER",
            Role::RegisteredDriver => "REGISTERED_DRIVER",
        }
    }

    pub fn try_from_str(s: &str) -> Option<Self> {
        match s {
            "ADMIN" => Some(Role::Admin),
            "PARTNER" => Some(Role::Partner),
            "REGISTERED_DRIVER" => Some(Role::RegisteredDriver),
            _ => None,
        }
    }

    pub fn all() -> [Role; 3] {
        [Role::Admin, Role::Partner, Role::RegisteredDriver]
    }

    pub fn hierarchy(&self) -> Vec<Role> {
        match self {
            Role::Admin => vec![Role::Admin, Role::Partner, Role::RegisteredDriver],
            Role::Partner => vec![Role::Partner, Role::RegisteredDriver],
            Role::RegisteredDriver => vec![Role::RegisteredDriver],
        }
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RoleSet {
    roles: HashSet<Role>,
}

impl RoleSet {
    pub fn new() -> Self {
        Self {
            roles: HashSet::new(),
        }
    }

    pub fn from_roles(roles: &[Role]) -> Self {
        let mut set = Self::new();
        set.roles.extend(roles.iter().cloned());
        set
    }

    pub fn from_strs(roles: &[&str]) -> Result<Self, AppError> {
        let mut set = Self::new();
        for role_str in roles {
            let role = Role::try_from_str(role_str)
                .ok_or_else(|| AppError::InvalidConfiguration(format!("Invalid role: {}", role_str)))?;
            set.roles.insert(role);
        }
        Ok(set)
    }

    pub fn add(&mut self, role: Role) {
        self.roles.insert(role);
    }

    pub fn remove(&mut self, role: Role) {
        self.roles.remove(&role);
    }

    pub fn contains(&self, role: Role) -> bool {
        self.roles.contains(&role)
    }

    pub fn contains_any(&self, roles: &[Role]) -> bool {
        roles.iter().any(|role| self.roles.contains(role))
    }

    pub fn contains_all(&self, roles: &[Role]) -> bool {
        roles.iter().all(|role| self.roles.contains(role))
    }

    pub fn is_empty(&self) -> bool {
        self.roles.is_empty()
    }

    pub fn len(&self) -> usize {
        self.roles.len()
    }

    pub fn roles(&self) -> impl Iterator<Item = &Role> {
        self.roles.iter()
    }

    pub fn to_vec(&self) -> Vec<Role> {
        self.roles.iter().cloned().collect()
    }
}

impl Default for RoleSet {
    fn default() -> Self {
        Self::new()
    }
}

pub struct RoleChecker {
    required_roles: Vec<Role>,
}

impl RoleChecker {
    pub fn require_role(role: Role) -> Self {
        Self {
            required_roles: vec![role],
        }
    }

    pub fn require_any_roles(roles: &[Role]) -> Self {
        Self {
            required_roles: roles.to_vec(),
        }
    }

    pub fn check(&self, user_roles: &RoleSet) -> Result<(), AppError> {
        if self.required_roles.is_empty() {
            return Ok(());
        }

        if self.required_roles.len() == 1 {
            let required_role = &self.required_roles[0];
            if user_roles.contains(*required_role) {
                return Ok(());
            }
        } else {
            if user_roles.contains_any(&self.required_roles) {
                return Ok(());
            }
        }

        Err(AppError::Forbidden)
    }
}

pub trait RoleGuard {
    fn has_role(&self, role: Role) -> bool;
    fn has_any_role(&self, roles: &[Role]) -> bool;
    fn has_all_roles(&self, roles: &[Role]) -> bool;
}

impl RoleGuard for RoleSet {
    fn has_role(&self, role: Role) -> bool {
        self.contains(role)
    }

    fn has_any_role(&self, roles: &[Role]) -> bool {
        self.contains_any(roles)
    }

    fn has_all_roles(&self, roles: &[Role]) -> bool {
        self.contains_all(roles)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_creation() {
        assert_eq!(Role::Admin.as_str(), "ADMIN");
        assert_eq!(Role::Partner.as_str(), "PARTNER");
        assert_eq!(Role::RegisteredDriver.as_str(), "REGISTERED_DRIVER");
    }

    #[test]
    fn role_from_str() {
        assert_eq!(Role::try_from_str("ADMIN"), Some(Role::Admin));
        assert_eq!(Role::try_from_str("PARTNER"), Some(Role::Partner));
        assert_eq!(Role::try_from_str("REGISTERED_DRIVER"), Some(Role::RegisteredDriver));
        assert_eq!(Role::try_from_str("DRIVER"), None); // Old role name should be rejected
        assert_eq!(Role::try_from_str("INVALID"), None);
    }

    #[test]
    fn role_hierarchy() {
        let admin_hierarchy = Role::Admin.hierarchy();
        assert!(admin_hierarchy.contains(&Role::Admin));
        assert!(admin_hierarchy.contains(&Role::Partner));
        assert!(admin_hierarchy.contains(&Role::RegisteredDriver));
        assert!(!admin_hierarchy.contains(&Role::System)); // System role removed

        let partner_hierarchy = Role::Partner.hierarchy();
        assert!(partner_hierarchy.contains(&Role::Partner));
        assert!(partner_hierarchy.contains(&Role::RegisteredDriver));
        assert!(!partner_hierarchy.contains(&Role::Admin));
        assert!(!partner_hierarchy.contains(&Role::System));
    }

    #[test]
    fn role_set_operations() {
        let mut set = RoleSet::new();
        assert!(set.is_empty());

        set.add(Role::Admin);
        assert!(!set.is_empty());
        assert_eq!(set.len(), 1);
        assert!(set.contains(Role::Admin));
        assert!(!set.contains(Role::Partner));

        set.add(Role::Partner);
        assert_eq!(set.len(), 2);
        assert!(set.contains(Role::Admin));
        assert!(set.contains(Role::Partner));

        set.remove(Role::Admin);
        assert_eq!(set.len(), 1);
        assert!(!set.contains(Role::Admin));
        assert!(set.contains(Role::Partner));
    }

    #[test]
    fn role_set_from_strs() {
        let set = RoleSet::from_strs(&["ADMIN", "PARTNER"]).unwrap();
        assert!(set.contains(Role::Admin));
        assert!(set.contains(Role::Partner));
        assert!(!set.contains(Role::RegisteredDriver));
    }

    #[test]
    fn role_set_from_strs_invalid_role() {
        let result = RoleSet::from_strs(&["ADMIN", "INVALID_ROLE"]);
        assert!(matches!(result, Err(AppError::InvalidConfiguration(_))));
    }

    #[test]
    fn role_set_contains_any() {
        let set = RoleSet::from_roles(&[Role::Admin, Role::Partner]);
        
        assert!(set.contains_any(&[Role::Admin]));
        assert!(set.contains_any(&[Role::Partner]));
        assert!(set.contains_any(&[Role::Admin, Role::RegisteredDriver]));
        assert!(!set.contains_any(&[Role::RegisteredDriver, Role::System])); // System role removed
    }

    #[test]
    fn role_set_contains_all() {
        let set = RoleSet::from_roles(&[Role::Admin, Role::Partner]);
        
        assert!(set.contains_all(&[Role::Admin]));
        assert!(set.contains_all(&[Role::Partner]));
        assert!(!set.contains_all(&[Role::Admin, Role::RegisteredDriver]));
        assert!(!set.contains_all(&[Role::Admin, Role::Partner, Role::RegisteredDriver]));
    }

    #[test]
    fn role_checker_single_role() {
        let checker = RoleChecker::require_role(Role::Admin);
        let admin_set = RoleSet::from_roles(&[Role::Admin]);
        let registered_driver_set = RoleSet::from_roles(&[Role::RegisteredDriver]);

        assert!(checker.check(&admin_set).is_ok());
        assert!(matches!(checker.check(&registered_driver_set), Err(AppError::Forbidden)));
    }

    #[test]
    fn role_checker_any_role() {
        let checker = RoleChecker::require_any_roles(&[Role::Admin, Role::Partner]);
        let admin_set = RoleSet::from_roles(&[Role::Admin]);
        let partner_set = RoleSet::from_roles(&[Role::Partner]);
        let registered_driver_set = RoleSet::from_roles(&[Role::RegisteredDriver]);

        assert!(checker.check(&admin_set).is_ok());
        assert!(checker.check(&partner_set).is_ok());
        assert!(matches!(checker.check(&registered_driver_set), Err(AppError::Forbidden)));
    }

    #[test]
    fn role_checker_no_roles_required() {
        let checker = RoleChecker {
            required_roles: vec![],
        };
        let empty_set = RoleSet::new();
        let admin_set = RoleSet::from_roles(&[Role::Admin]);

        assert!(checker.check(&empty_set).is_ok());
        assert!(checker.check(&admin_set).is_ok());
    }

    #[test]
    fn role_guard_trait() {
        let set = RoleSet::from_roles(&[Role::Admin, Role::Partner]);

        assert!(set.has_role(Role::Admin));
        assert!(!set.has_role(Role::RegisteredDriver));

        assert!(set.has_any_role(&[Role::Admin]));
        assert!(set.has_any_role(&[Role::Partner]));
        assert!(set.has_any_role(&[Role::Admin, Role::RegisteredDriver]));
        assert!(!set.has_any_role(&[Role::RegisteredDriver, Role::System])); // System role removed

        assert!(set.has_all_roles(&[Role::Admin]));
        assert!(set.has_all_roles(&[Role::Partner]));
        assert!(!set.has_all_roles(&[Role::Admin, Role::RegisteredDriver]));
    }

    #[test]
    fn role_all() {
        let all_roles = Role::all();
        assert_eq!(all_roles.len(), 3);
        assert!(all_roles.contains(&Role::Admin));
        assert!(all_roles.contains(&Role::Partner));
        assert!(all_roles.contains(&Role::RegisteredDriver));
        assert!(!all_roles.contains(&Role::System)); // System role removed
    }
}
