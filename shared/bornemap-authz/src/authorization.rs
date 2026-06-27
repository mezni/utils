use crate::guards::{RoleGuard, OwnershipGuard, AuthorizationError as GuardAuthorizationError};
use crate::policies::Policy;
use bornemap_auth::rbac::Role;
use bornemap_core::AppError;
use std::collections::HashMap;

/// Simple role set implementation for authorization
#[derive(Debug, Clone)]
pub struct RoleSet {
    roles: Vec<Role>,
}

impl RoleSet {
    pub fn new() -> Self {
        Self { roles: Vec::new() }
    }

    pub fn from_roles(roles: &[Role]) -> Self {
        Self { roles: roles.to_vec() }
    }

    pub fn contains(&self, role: Role) -> bool {
        self.roles.contains(&role)
    }

    pub fn contains_any(&self, roles: &[Role]) -> bool {
        roles.iter().any(|&role| self.contains(role))
    }

    pub fn contains_all(&self, roles: &[Role]) -> bool {
        roles.iter().all(|&role| self.contains(role))
    }

    pub fn add_role(&mut self, role: Role) {
        if !self.contains(role) {
            self.roles.push(role);
        }
    }

    pub fn remove_role(&mut self, role: Role) {
        self.roles.retain(|&r| r != role);
    }

    pub fn get_roles(&self) -> &[Role] {
        &self.roles
    }

    pub fn is_empty(&self) -> bool {
        self.roles.is_empty()
    }

    pub fn len(&self) -> usize {
        self.roles.len()
    }
}

impl Default for RoleSet {
    fn default() -> Self {
        Self::new()
    }
}

/// Authorization context containing all information needed for authorization decisions
#[derive(Debug, Clone)]
pub struct AuthorizationContext {
    pub user_id: uuid::Uuid,
    pub roles: RoleSet,
    pub ownership: HashMap<String, crate::ownership::ResourceOwnership>,
}

impl AuthorizationContext {
    pub fn new(user_id: uuid::Uuid, roles: RoleSet) -> Self {
        Self {
            user_id,
            roles,
            ownership: HashMap::new(),
        }
    }

    pub fn with_ownership(mut self, resource_type: &str, ownership: crate::ownership::ResourceOwnership) -> Self {
        self.ownership.insert(resource_type.to_string(), ownership);
        self
    }

    pub fn has_role(&self, role: Role) -> bool {
        self.roles.contains(role)
    }

    pub fn has_any_role(&self, roles: &[Role]) -> bool {
        self.roles.contains_any(roles)
    }

    pub fn has_all_roles(&self, roles: &[Role]) -> bool {
        self.roles.contains_all(roles)
    }

    pub fn get_ownership(&self, resource_type: &str) -> Option<&crate::ownership::ResourceOwnership> {
        self.ownership.get(resource_type)
    }
}

/// Authorization error types
#[derive(Debug, thiserror::Error)]
pub enum AuthorizationError {
    #[error("Access denied: insufficient permissions")]
    InsufficientPermissions,
    #[error("Access denied: resource ownership required")]
    OwnershipRequired,
    #[error("Access denied: policy violation")]
    PolicyViolation,
    #[error("Authorization configuration error: {0}")]
    Configuration(String),
}

impl From<crate::guards::AuthorizationError> for AuthorizationError {
    fn from(err: crate::guards::AuthorizationError) -> Self {
        match err {
            crate::guards::AuthorizationError::InsufficientPermissions => AuthorizationError::InsufficientPermissions,
            crate::guards::AuthorizationError::OwnershipRequired => AuthorizationError::OwnershipRequired,
            crate::guards::AuthorizationError::PolicyViolation => AuthorizationError::PolicyViolation,
            crate::guards::AuthorizationError::Configuration(msg) => AuthorizationError::Configuration(msg),
        }
    }
}

impl From<AuthorizationError> for AppError {
    fn from(err: AuthorizationError) -> Self {
        match err {
            AuthorizationError::InsufficientPermissions => AppError::Forbidden,
            AuthorizationError::OwnershipRequired => AppError::Forbidden,
            AuthorizationError::PolicyViolation => AppError::Forbidden,
            AuthorizationError::Configuration(msg) => AppError::InvalidConfiguration(msg),
        }
    }
}

/// Main authorization service that combines role, ownership, and policy checks
pub struct AuthorizationService {
    role_guards: HashMap<String, Box<dyn RoleGuard>>,
    ownership_guards: HashMap<String, Box<dyn OwnershipGuard>>,
    policies: HashMap<String, Box<dyn Policy>>,
}

impl AuthorizationService {
    pub fn new() -> Self {
        Self {
            role_guards: HashMap::new(),
            ownership_guards: HashMap::new(),
            policies: HashMap::new(),
        }
    }

    pub fn register_role_guard<T: RoleGuard + 'static>(&mut self, name: &str, guard: T) {
        self.role_guards.insert(name.to_string(), Box::new(guard));
    }

    pub fn register_ownership_guard<T: OwnershipGuard + 'static>(&mut self, name: &str, guard: T) {
        self.ownership_guards.insert(name.to_string(), Box::new(guard));
    }

    pub fn register_policy<T: Policy + 'static>(&mut self, name: &str, policy: T) {
        self.policies.insert(name.to_string(), Box::new(policy));
    }

    /// Authorize access based on role requirements
    pub fn authorize_by_role(
        &self,
        context: &AuthorizationContext,
        required_roles: &[Role],
        guard_name: &str,
    ) -> Result<(), AuthorizationError> {
        let guard = self.role_guards.get(guard_name)
            .ok_or_else(|| AuthorizationError::Configuration(format!("Role guard '{}' not found", guard_name)))?;

        guard.check_roles(context, required_roles)
            .map_err(GuardAuthorizationError::into)
    }

    /// Authorize access based on resource ownership
    pub fn authorize_by_ownership(
        &self,
        context: &AuthorizationContext,
        resource_type: &str,
        resource_id: &str,
        guard_name: &str,
    ) -> Result<(), AuthorizationError> {
        let guard = self.ownership_guards.get(guard_name)
            .ok_or_else(|| AuthorizationError::Configuration(format!("Ownership guard '{}' not found", guard_name)))?;

        guard.check_ownership(context, resource_type, resource_id)
            .map_err(GuardAuthorizationError::into)
    }

    /// Authorize access using a policy
    pub fn authorize_by_policy(
        &self,
        context: &AuthorizationContext,
        policy_name: &str,
        resource_type: &str,
        resource_id: &str,
    ) -> Result<(), AuthorizationError> {
        let policy = self.policies.get(policy_name)
            .ok_or_else(|| AuthorizationError::Configuration(format!("Policy '{}' not found", policy_name)))?;

        policy.evaluate(context, resource_type, resource_id)
            .map_err(|_| AuthorizationError::PolicyViolation)?;
        
        Ok(())
    }

    /// Comprehensive authorization check combining all methods
    pub fn authorize(
        &self,
        context: &AuthorizationContext,
        resource_type: &str,
        resource_id: &str,
        required_roles: &[Role],
        role_guard_name: &str,
        ownership_guard_name: &str,
        policy_name: &str,
    ) -> Result<(), AuthorizationError> {
        // First check role requirements
        self.authorize_by_role(context, required_roles, role_guard_name)?;

        // Then check ownership if required
        if !required_roles.contains(&Role::Admin) {
            // Admin bypasses ownership checks
            self.authorize_by_ownership(context, resource_type, resource_id, ownership_guard_name)?;
        }

        // Finally check policy
        self.authorize_by_policy(context, policy_name, resource_type, resource_id)?;

        Ok(())
    }
}

impl Default for AuthorizationService {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bornemap_auth::rbac::Role;

    #[test]
    fn authorization_context_creation() {
        let user_id = uuid::Uuid::new_v4();
        let roles = RoleSet::from_roles(&[Role::Admin]);
        let context = AuthorizationContext::new(user_id, roles);

        assert!(context.has_role(Role::Admin));
        assert!(!context.has_role(Role::Partner));
    }

    #[test]
    fn authorization_context_with_ownership() {
        let user_id = uuid::Uuid::new_v4();
        let roles = RoleSet::from_roles(&[Role::Partner]);
        let mut context = AuthorizationContext::new(user_id, roles);

        let ownership = crate::ownership::ResourceOwnership::Owner(
            crate::ownership::Owner::User(user_id)
        );
        
        context = context.with_ownership("station", ownership);
        
        assert!(context.get_ownership("station").is_some());
        assert!(context.get_ownership("charger").is_none());
    }

    #[test]
    fn authorization_service_role_check() {
        let mut service = AuthorizationService::new();
        
        // Mock role guard that always allows
        struct MockRoleGuard;
        impl RoleGuard for MockRoleGuard {
            fn check_roles(&self, _context: &AuthorizationContext, _roles: &[Role]) -> Result<(), AuthorizationError> {
                Ok(())
            }
        }
        
        service.register_role_guard("admin", MockRoleGuard);
        
        let user_id = uuid::Uuid::new_v4();
        let roles = RoleSet::from_roles(&[Role::Admin]);
        let context = AuthorizationContext::new(user_id, roles);
        
        let result = service.authorize_by_role(&context, &[Role::Admin], "admin");
        assert!(result.is_ok());
    }

    #[test]
    fn authorization_service_ownership_check() {
        let mut service = AuthorizationService::new();
        
        // Mock ownership guard that always allows
        struct MockOwnershipGuard;
        impl OwnershipGuard for MockOwnershipGuard {
            fn check_ownership(&self, _context: &AuthorizationContext, _resource_type: &str, _resource_id: &str) -> Result<(), AuthorizationError> {
                Ok(())
            }
        }
        
        service.register_ownership_guard("station", MockOwnershipGuard);
        
        let user_id = uuid::Uuid::new_v4();
        let roles = RoleSet::from_roles(&[Role::Partner]);
        let context = AuthorizationContext::new(user_id, roles);
        
        let result = service.authorize_by_ownership(&context, "station", "station-123", "station");
        assert!(result.is_ok());
    }

    #[test]
    fn authorization_service_policy_check() {
        let mut service = AuthorizationService::new();
        
        // Mock policy that always allows
        struct MockPolicy;
        impl Policy for MockPolicy {
            fn evaluate(&self, _context: &AuthorizationContext, _resource_type: &str, _resource_id: &str) -> Result<(), PolicyError> {
                Ok(())
            }
        }
        
        service.register_policy("station_management", MockPolicy);
        
        let user_id = uuid::Uuid::new_v4();
        let roles = RoleSet::from_roles(&[Role::Admin]);
        let context = AuthorizationContext::new(user_id, roles);
        
        let result = service.authorize_by_policy(&context, "station_management", "station", "station-123");
        assert!(result.is_ok());
    }
}