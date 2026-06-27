use crate::authorization::AuthorizationContext;
use bornemap_auth::rbac::Role;
use bornemap_core::AppError;
use std::collections::HashMap;

/// Trait for authorization policies
pub trait Policy: Send + Sync {
    /// Evaluate the policy against the authorization context
    fn evaluate(&self, context: &AuthorizationContext, resource_type: &str, resource_id: &str) -> Result<(), PolicyError>;
}

/// Policy error types
#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    #[error("Policy evaluation failed: {0}")]
    Evaluation(String),
    #[error("Policy configuration error: {0}")]
    Configuration(String),
    #[error("Policy not found: {0}")]
    NotFound(String),
}

impl From<PolicyError> for AppError {
    fn from(err: PolicyError) -> Self {
        match err {
            PolicyError::Evaluation(msg) => AppError::Forbidden,
            PolicyError::Configuration(msg) => AppError::InvalidConfiguration(msg),
            PolicyError::NotFound(msg) => AppError::Forbidden,
        }
    }
}

/// Base policy implementation
pub struct BasePolicy {
    name: String,
    description: String,
}

impl BasePolicy {
    pub fn new(name: String, description: String) -> Self {
        Self {
            name,
            description,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn description(&self) -> &str {
        &self.description
    }
}

/// Role-based policy
pub struct RolePolicy {
    base: BasePolicy,
    required_roles: Vec<Role>,
    allow_any: bool,
}

impl RolePolicy {
    pub fn new(required_roles: Vec<Role>, allow_any: bool) -> Self {
        Self {
            base: BasePolicy::new(
                "Role-based".to_string(),
                if allow_any {
                    "User must have any of the specified roles".to_string()
                } else {
                    "User must have all specified roles".to_string()
                },
            ),
            required_roles,
            allow_any,
        }
    }

    pub fn required_roles(&self) -> &[Role] {
        &self.required_roles
    }

    pub fn allow_any(&self) -> bool {
        self.allow_any
    }
}

impl Policy for RolePolicy {
    fn evaluate(&self, context: &AuthorizationContext, _resource_type: &str, _resource_id: &str) -> Result<(), PolicyError> {
        if self.required_roles.is_empty() {
            return Ok(());
        }

        if self.allow_any {
            if context.has_any_role(&self.required_roles) {
                Ok(())
            } else {
                Err(PolicyError::Evaluation(format!(
                    "User does not have any of the required roles: {:?}",
                    self.required_roles
                )))
            }
        } else {
            if context.has_all_roles(&self.required_roles) {
                Ok(())
            } else {
                Err(PolicyError::Evaluation(format!(
                    "User does not have all required roles: {:?}",
                    self.required_roles
                )))
            }
        }
    }
}

/// Ownership-based policy
pub struct OwnershipPolicy {
    base: BasePolicy,
    require_ownership: bool,
}

impl OwnershipPolicy {
    pub fn new(require_ownership: bool) -> Self {
        Self {
            base: BasePolicy::new(
                "Ownership-based".to_string(),
                if require_ownership {
                    "User must own the resource".to_string()
                } else {
                    "Resource can be accessed by anyone".to_string()
                },
            ),
            require_ownership,
        }
    }

    pub fn require_ownership(&self) -> bool {
        self.require_ownership
    }
}

impl Policy for OwnershipPolicy {
    fn evaluate(&self, context: &AuthorizationContext, resource_type: &str, resource_id: &str) -> Result<(), PolicyError> {
        if !self.require_ownership {
            return Ok(());
        }

        let ownership = context.get_ownership(resource_type)
            .ok_or_else(|| PolicyError::Evaluation(format!("Ownership information not available for {}", resource_type)))?;

        match ownership {
            crate::ownership::ResourceOwnership::Owner(owner) => {
                match owner {
                    crate::ownership::Owner::User(user_id) => {
                        if user_id == &context.user_id {
                            Ok(())
                        } else {
                            Err(PolicyError::Evaluation(format!(
                                "User {} does not own resource {}",
                                context.user_id,
                                resource_id
                            )))
                        }
                    },
                    crate::ownership::Owner::Partner(_) => {
                        // In a real implementation, we'd check if the user belongs to this partner
                        // For now, we'll assume partners can access their own resources
                        if context.has_role(Role::Partner) {
                            Ok(())
                        } else {
                            Err(PolicyError::Evaluation(format!(
                                "User {} does not have partner role for resource {}",
                                context.user_id,
                                resource_id
                            )))
                        }
                    },
                    crate::ownership::Owner::System => {
                        Err(PolicyError::Evaluation(format!(
                            "System-owned resource {} cannot be owned by user",
                            resource_id
                        )))
                    },
                }
            },
            crate::ownership::ResourceOwnership::Shared => {
                Ok(())
            },
        }
    }
}

/// Admin bypass policy
pub struct AdminBypassPolicy {
    base: BasePolicy,
}

impl AdminBypassPolicy {
    pub fn new() -> Self {
        Self {
            base: BasePolicy::new(
                "Admin Bypass".to_string(),
                "Admin users bypass all restrictions".to_string(),
            ),
        }
    }
}

impl Policy for AdminBypassPolicy {
    fn evaluate(&self, context: &AuthorizationContext, _resource_type: &str, _resource_id: &str) -> Result<(), PolicyError> {
        if context.has_role(Role::Admin) {
            Ok(())
        } else {
            Err(PolicyError::Evaluation("Admin role required for this resource".to_string()))
        }
    }
}

/// Combined policy that evaluates multiple policies
pub struct CombinedPolicy {
    base: BasePolicy,
    policies: Vec<Box<dyn Policy>>,
    mode: PolicyCombinationMode,
}

#[derive(Debug, Clone)]
pub enum PolicyCombinationMode {
    AllMustPass, // All policies must pass (AND)
    AnyMustPass, // Any policy must pass (OR)
}

impl CombinedPolicy {
    pub fn new(name: String, description: String, policies: Vec<Box<dyn Policy>>, mode: PolicyCombinationMode) -> Self {
        Self {
            base: BasePolicy::new(name, description),
            policies,
            mode,
        }
    }

    pub fn policies(&self) -> &[Box<dyn Policy>] {
        &self.policies
    }

    pub fn mode(&self) -> &PolicyCombinationMode {
        &self.mode
    }
}

impl Policy for CombinedPolicy {
    fn evaluate(&self, context: &AuthorizationContext, resource_type: &str, resource_id: &str) -> Result<(), PolicyError> {
        match self.mode {
            PolicyCombinationMode::AllMustPass => {
                for policy in &self.policies {
                    policy.evaluate(context, resource_type, resource_id)?;
                }
                Ok(())
            },
            PolicyCombinationMode::AnyMustPass => {
                let mut errors = Vec::new();
                for policy in &self.policies {
                    match policy.evaluate(context, resource_type, resource_id) {
                        Ok(()) => return Ok(()),
                        Err(e) => errors.push(e),
                    }
                }
                Err(PolicyError::Evaluation(format!(
                    "No policies passed. Errors: {:?}",
                    errors
                )))
            },
        }
    }
}

/// Policy registry for managing policies
pub struct PolicyRegistry {
    policies: HashMap<String, Box<dyn Policy>>,
}

impl PolicyRegistry {
    pub fn new() -> Self {
        Self {
            policies: HashMap::new(),
        }
    }

    /// Register a policy
    pub fn register(&mut self, name: String, policy: Box<dyn Policy>) {
        self.policies.insert(name, policy);
    }

    /// Get a policy by name
    pub fn get(&self, name: &str) -> Result<&dyn Policy, PolicyError> {
        self.policies.get(name)
            .map(|p| p.as_ref())
            .ok_or_else(|| PolicyError::NotFound(format!("Policy '{}' not found", name)))
    }

    /// List all registered policies
    pub fn list(&self) -> Vec<String> {
        self.policies.keys().cloned().collect()
    }

    /// Remove a policy
    pub fn remove(&mut self, name: &str) -> Option<Box<dyn Policy>> {
        self.policies.remove(name)
    }
}

impl Default for PolicyRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Factory for creating common policies
pub struct PolicyFactory;

impl PolicyFactory {
    /// Create a role policy
    pub fn role_policy(required_roles: Vec<Role>, allow_any: bool) -> Box<dyn Policy> {
        Box::new(RolePolicy::new(required_roles, allow_any))
    }

    /// Create an ownership policy
    pub fn ownership_policy(require_ownership: bool) -> Box<dyn Policy> {
        Box::new(OwnershipPolicy::new(require_ownership))
    }

    /// Create an admin bypass policy
    pub fn admin_bypass_policy() -> Box<dyn Policy> {
        Box::new(AdminBypassPolicy::new())
    }

    /// Create a combined policy
    pub fn combined_policy(
        name: String,
        description: String,
        policies: Vec<Box<dyn Policy>>,
        mode: PolicyCombinationMode,
    ) -> Box<dyn Policy> {
        Box::new(CombinedPolicy::new(name, description, policies, mode))
    }

    /// Create a policy that requires admin role
    pub fn admin_only_policy() -> Box<dyn Policy> {
        Self::admin_bypass_policy()
    }

    /// Create a policy that requires partner role
    pub fn partner_only_policy() -> Box<dyn Policy> {
        Self::role_policy(vec![Role::Partner], false)
    }

    /// Create a policy that requires registered driver role
    pub fn driver_only_policy() -> Box<dyn Policy> {
        Self::role_policy(vec![Role::RegisteredDriver], false)
    }

    /// Create a policy that allows any registered user
    pub fn registered_user_policy() -> Box<dyn Policy> {
        Self::role_policy(vec![Role::Admin, Role::Partner, Role::RegisteredDriver], true)
    }

    /// Create a policy for station access (driver or partner ownership)
    pub fn station_access_policy() -> Box<dyn Policy> {
        Self::combined_policy(
            "Station Access".to_string(),
            "Access to station requires driver role or ownership".to_string(),
            vec![
                Self::role_policy(vec![Role::RegisteredDriver], false),
                Self::ownership_policy(false),
            ],
            PolicyCombinationMode::AnyMustPass,
        )
    }

    /// Create a policy for partner management (admin or partner ownership)
    pub fn partner_management_policy() -> Box<dyn Policy> {
        Self::combined_policy(
            "Partner Management".to_string(),
            "Partner management requires admin role or partner ownership".to_string(),
            vec![
                Self::admin_bypass_policy(),
                Self::ownership_policy(false),
            ],
            PolicyCombinationMode::AnyMustPass,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bornemap_auth::rbac::Role;

    #[test]
    fn role_policy_any() {
        let policy = PolicyFactory::role_policy(vec![Role::Admin, Role::Partner], true);
        let user_id = uuid::Uuid::new_v4();
        
        // User with admin role
        let roles = RoleSet::from_roles(&[Role::Admin]);
        let context = AuthorizationContext::new(user_id, roles);
        assert!(policy.evaluate(&context, "station", "123").is_ok());

        // User with partner role
        let roles = RoleSet::from_roles(&[Role::Partner]);
        let context = AuthorizationContext::new(user_id, roles);
        assert!(policy.evaluate(&context, "station", "123").is_ok());

        // User with driver role
        let roles = RoleSet::from_roles(&[Role::RegisteredDriver]);
        let context = AuthorizationContext::new(user_id, roles);
        assert!(policy.evaluate(&context, "station", "123").is_err());
    }

    #[test]
    fn role_policy_all() {
        let policy = PolicyFactory::role_policy(vec![Role::Admin, Role::Partner], false);
        let user_id = uuid::Uuid::new_v4();
        
        // User with both roles
        let roles = RoleSet::from_roles(&[Role::Admin, Role::Partner]);
        let context = AuthorizationContext::new(user_id, roles);
        assert!(policy.evaluate(&context, "station", "123").is_ok());

        // User with only admin role
        let roles = RoleSet::from_roles(&[Role::Admin]);
        let context = AuthorizationContext::new(user_id, roles);
        assert!(policy.evaluate(&context, "station", "123").is_err());

        // User with only partner role
        let roles = RoleSet::from_roles(&[Role::Partner]);
        let context = AuthorizationContext::new(user_id, roles);
        assert!(policy.evaluate(&context, "station", "123").is_err());
    }

    #[test]
    fn ownership_policy() {
        let policy = PolicyFactory::ownership_policy(true);
        let user_id = uuid::Uuid::new_v4();
        
        // Context with user ownership
        let mut context = AuthorizationContext::new(user_id, RoleSet::from_roles(&[Role::Partner]));
        let ownership = crate::ownership::ResourceOwnership::Owner(
            crate::ownership::Owner::User(user_id)
        );
        context = context.with_ownership("station", ownership);

        // Should pass for owned resource
        assert!(policy.evaluate(&context, "station", "123").is_ok());

        // Should fail for unowned resource
        let context = AuthorizationContext::new(user_id, RoleSet::from_roles(&[Role::Partner]));
        assert!(policy.evaluate(&context, "station", "123").is_err());
    }

    #[test]
    fn admin_bypass_policy() {
        let policy = PolicyFactory::admin_bypass_policy();
        let user_id = uuid::Uuid::new_v4();
        
        // Admin user
        let roles = RoleSet::from_roles(&[Role::Admin]);
        let context = AuthorizationContext::new(user_id, roles);
        assert!(policy.evaluate(&context, "station", "123").is_ok());

        // Non-admin user
        let roles = RoleSet::from_roles(&[Role::Partner]);
        let context = AuthorizationContext::new(user_id, roles);
        assert!(policy.evaluate(&context, "station", "123").is_err());
    }

    #[test]
    fn combined_policy_all() {
        let policy = PolicyFactory::combined_policy(
            "Test Policy".to_string(),
            "Test description".to_string(),
            vec![
                PolicyFactory::role_policy(vec![Role::Admin], false),
                PolicyFactory::ownership_policy(true),
            ],
            PolicyCombinationMode::AllMustPass,
        );
        let user_id = uuid::Uuid::new_v4();
        
        // Admin with ownership
        let roles = RoleSet::from_roles(&[Role::Admin]);
        let mut context = AuthorizationContext::new(user_id, roles);
        let ownership = crate::ownership::ResourceOwnership::Owner(
            crate::ownership::Owner::User(user_id)
        );
        context = context.with_ownership("station", ownership);
        assert!(policy.evaluate(&context, "station", "123").is_ok());

        // Admin without ownership
        let context = AuthorizationContext::new(user_id, RoleSet::from_roles(&[Role::Admin]));
        assert!(policy.evaluate(&context, "station", "123").is_err());

        // Non-admin with ownership
        let roles = RoleSet::from_roles(&[Role::Partner]);
        let mut context = AuthorizationContext::new(user_id, roles);
        let ownership = crate::ownership::ResourceOwnership::Owner(
            crate::ownership::Owner::User(user_id)
        );
        context = context.with_ownership("station", ownership);
        assert!(policy.evaluate(&context, "station", "123").is_err());
    }

    #[test]
    fn combined_policy_any() {
        let policy = PolicyFactory::combined_policy(
            "Test Policy".to_string(),
            "Test description".to_string(),
            vec![
                PolicyFactory::role_policy(vec![Role::Admin], false),
                PolicyFactory::ownership_policy(true),
            ],
            PolicyCombinationMode::AnyMustPass,
        );
        let user_id = uuid::Uuid::new_v4();
        
        // Admin without ownership
        let context = AuthorizationContext::new(user_id, RoleSet::from_roles(&[Role::Admin]));
        assert!(policy.evaluate(&context, "station", "123").is_ok());

        // Non-admin with ownership
        let roles = RoleSet::from_roles(&[Role::Partner]);
        let mut context = AuthorizationContext::new(user_id, roles);
        let ownership = crate::ownership::ResourceOwnership::Owner(
            crate::ownership::Owner::User(user_id)
        );
        context = context.with_ownership("station", ownership);
        assert!(policy.evaluate(&context, "station", "123").is_ok());

        // Non-admin without ownership
        let context = AuthorizationContext::new(user_id, RoleSet::from_roles(&[Role::Partner]));
        assert!(policy.evaluate(&context, "station", "123").is_err());
    }

    #[test]
    fn policy_registry() {
        let mut registry = PolicyRegistry::new();
        
        // Register policies
        registry.register("admin".to_string(), PolicyFactory::admin_only_policy());
        registry.register("partner".to_string(), PolicyFactory::partner_only_policy());
        
        // List policies
        let policies = registry.list();
        assert!(policies.contains(&"admin".to_string()));
        assert!(policies.contains(&"partner".to_string()));
        
        // Get policies
        let admin_policy = registry.get("admin").unwrap();
        let partner_policy = registry.get("partner").unwrap();
        
        // Test policies
        let user_id = uuid::Uuid::new_v4();
        let admin_context = AuthorizationContext::new(user_id, RoleSet::from_roles(&[Role::Admin]));
        let partner_context = AuthorizationContext::new(user_id, RoleSet::from_roles(&[Role::Partner]));
        
        assert!(admin_policy.evaluate(&admin_context, "station", "123").is_ok());
        assert!(admin_policy.evaluate(&partner_context, "station", "123").is_err());
        
        assert!(partner_policy.evaluate(&admin_context, "station", "123").is_ok());
        assert!(partner_policy.evaluate(&partner_context, "station", "123").is_ok());
        
        // Remove policy
        let removed = registry.remove("admin");
        assert!(removed.is_some());
        
        // Should not find removed policy
        assert!(registry.get("admin").is_err());
    }
}