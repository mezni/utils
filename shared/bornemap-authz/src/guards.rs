use crate::authorization::AuthorizationContext;
use bornemap_auth::rbac::Role;
use bornemap_core::AppError;

/// Trait for role-based authorization guards
pub trait RoleGuard: Send + Sync {
    /// Check if user has required roles
    fn check_roles(&self, context: &AuthorizationContext, required_roles: &[Role]) -> Result<(), AuthorizationError>;
}

/// Default role guard that implements basic role checking
pub struct DefaultRoleGuard;

impl RoleGuard for DefaultRoleGuard {
    fn check_roles(&self, context: &AuthorizationContext, required_roles: &[Role]) -> Result<(), AuthorizationError> {
        if required_roles.is_empty() {
            return Ok(());
        }

        if required_roles.len() == 1 {
            let required_role = &required_roles[0];
            if context.has_role(*required_role) {
                return Ok(());
            }
        } else {
            if context.has_any_role(required_roles) {
                return Ok(());
            }
        }

        Err(AuthorizationError::InsufficientPermissions)
    }
}

/// Trait for ownership-based authorization guards
pub trait OwnershipGuard: Send + Sync {
    /// Check if user owns the specified resource
    fn check_ownership(&self, context: &AuthorizationContext, resource_type: &str, resource_id: &str) -> Result<(), AuthorizationError>;
}

/// Default ownership guard that checks user ownership
pub struct DefaultOwnershipGuard;

impl OwnershipGuard for DefaultOwnershipGuard {
    fn check_ownership(&self, context: &AuthorizationContext, resource_type: &str, resource_id: &str) -> Result<(), AuthorizationError> {
        let ownership = context.get_ownership(resource_type)
            .ok_or_else(|| AuthorizationError::OwnershipRequired)?;

        match ownership {
            crate::ownership::ResourceOwnership::Owner(owner) => {
                match owner {
                    crate::ownership::Owner::User(user_id) => {
                        // User owns the resource if it's their own ID
                        if user_id == &context.user_id {
                            Ok(())
                        } else {
                            Err(AuthorizationError::OwnershipRequired)
                        }
                    },
                    crate::ownership::Owner::Partner(partner_id) => {
                        // Partner owns the resource if they're the partner
                        // This would need to be mapped to user's partner ID in a real implementation
                        // For now, we'll assume the user is a partner and check their partner ID
                        if context.has_role(Role::Partner) {
                            // In a real implementation, we'd check if the user belongs to this partner
                            Ok(())
                        } else {
                            Err(AuthorizationError::OwnershipRequired)
                        }
                    },
                    crate::ownership::Owner::System => {
                        // System-owned resources are not user-ownable
                        Err(AuthorizationError::OwnershipRequired)
                    },
                }
            },
            crate::ownership::ResourceOwnership::Shared => {
                // Shared resources are accessible to all
                Ok(())
            },
        }
    }
}

/// Admin ownership guard that bypasses ownership checks for admin users
pub struct AdminOwnershipGuard;

impl OwnershipGuard for AdminOwnershipGuard {
    fn check_ownership(&self, context: &AuthorizationContext, _resource_type: &str, _resource_id: &str) -> Result<(), AuthorizationError> {
        // Admin users bypass all ownership checks
        if context.has_role(Role::Admin) {
            Ok(())
        } else {
            // For non-admin users, use the default ownership check
            DefaultOwnershipGuard.check_ownership(context, _resource_type, _resource_id)
        }
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

/// Factory for creating common authorization guards
pub struct GuardFactory;

impl GuardFactory {
    /// Create a role guard that requires specific roles
    pub fn role_guard(required_roles: Vec<Role>) -> impl RoleGuard {
        RequiredRoleGuard { required_roles }
    }

    /// Create a role guard that allows any of the specified roles
    pub fn any_role_guard(allowed_roles: Vec<Role>) -> impl RoleGuard {
        AnyRoleGuard { allowed_roles }
    }

    /// Create a default ownership guard
    pub fn ownership_guard() -> impl OwnershipGuard {
        DefaultOwnershipGuard
    }

    /// Create an admin bypass ownership guard
    pub fn admin_ownership_guard() -> impl OwnershipGuard {
        AdminOwnershipGuard
    }
}

/// Role guard that requires all specified roles
struct RequiredRoleGuard {
    required_roles: Vec<Role>,
}

impl RoleGuard for RequiredRoleGuard {
    fn check_roles(&self, context: &AuthorizationContext, _required_roles: &[Role]) -> Result<(), AuthorizationError> {
        if context.has_all_roles(&self.required_roles) {
            Ok(())
        } else {
            Err(AuthorizationError::InsufficientPermissions)
        }
    }
}

/// Role guard that allows any of the specified roles
struct AnyRoleGuard {
    allowed_roles: Vec<Role>,
}

impl RoleGuard for AnyRoleGuard {
    fn check_roles(&self, context: &AuthorizationContext, _required_roles: &[Role]) -> Result<(), AuthorizationError> {
        if context.has_any_role(&self.allowed_roles) {
            Ok(())
        } else {
            Err(AuthorizationError::InsufficientPermissions)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bornemap_auth::rbac::Role;

    #[test]
    fn default_role_guard_single_role() {
        let guard = DefaultRoleGuard;
        let user_id = uuid::Uuid::new_v4();
        let roles = RoleSet::from_roles(&[Role::Admin]);
        let context = AuthorizationContext::new(user_id, roles);

        // Should pass for admin role
        let result = guard.check_roles(&context, &[Role::Admin]);
        assert!(result.is_ok());

        // Should fail for partner role
        let result = guard.check_roles(&context, &[Role::Partner]);
        assert!(matches!(result, Err(AuthorizationError::InsufficientPermissions)));
    }

    #[test]
    fn default_role_guard_multiple_roles() {
        let guard = DefaultRoleGuard;
        let user_id = uuid::Uuid::new_v4();
        let roles = RoleSet::from_roles(&[Role::Partner]);
        let context = AuthorizationContext::new(user_id, roles);

        // Should pass for any role (admin or partner)
        let result = guard.check_roles(&context, &[Role::Admin, Role::Partner]);
        assert!(result.is_ok());

        // Should fail for driver role
        let result = guard.check_roles(&context, &[Role::RegisteredDriver]);
        assert!(matches!(result, Err(AuthorizationError::InsufficientPermissions)));
    }

    #[test]
    fn required_role_guard() {
        let guard = GuardFactory::role_guard(vec![Role::Admin, Role::Partner]);
        let user_id = uuid::Uuid::new_v4();
        
        // User with both roles
        let roles = RoleSet::from_roles(&[Role::Admin, Role::Partner]);
        let context = AuthorizationContext::new(user_id, roles);
        let result = guard.check_roles(&context, &[]);
        assert!(result.is_ok());

        // User with only admin role
        let roles = RoleSet::from_roles(&[Role::Admin]);
        let context = AuthorizationContext::new(user_id, roles);
        let result = guard.check_roles(&context, &[]);
        assert!(result.is_ok());

        // User with only partner role
        let roles = RoleSet::from_roles(&[Role::Partner]);
        let context = AuthorizationContext::new(user_id, roles);
        let result = guard.check_roles(&context, &[]);
        assert!(result.is_ok());

        // User with only driver role
        let roles = RoleSet::from_roles(&[Role::RegisteredDriver]);
        let context = AuthorizationContext::new(user_id, roles);
        let result = guard.check_roles(&context, &[]);
        assert!(matches!(result, Err(AuthorizationError::InsufficientPermissions)));
    }

    #[test]
    fn any_role_guard() {
        let guard = GuardFactory::any_role_guard(vec![Role::Admin, Role::Partner]);
        let user_id = uuid::Uuid::new_v4();
        
        // User with admin role
        let roles = RoleSet::from_roles(&[Role::Admin]);
        let context = AuthorizationContext::new(user_id, roles);
        let result = guard.check_roles(&context, &[]);
        assert!(result.is_ok());

        // User with partner role
        let roles = RoleSet::from_roles(&[Role::Partner]);
        let context = AuthorizationContext::new(user_id, roles);
        let result = guard.check_roles(&context, &[]);
        assert!(result.is_ok());

        // User with driver role
        let roles = RoleSet::from_roles(&[Role::RegisteredDriver]);
        let context = AuthorizationContext::new(user_id, roles);
        let result = guard.check_roles(&context, &[]);
        assert!(matches!(result, Err(AuthorizationError::InsufficientPermissions)));
    }

    #[test]
    fn default_ownership_guard_user_ownership() {
        let guard = DefaultOwnershipGuard;
        let user_id = uuid::Uuid::new_v4();
        let roles = RoleSet::from_roles(&[Role::Partner]);
        
        // Context with user ownership
        let mut context = AuthorizationContext::new(user_id, roles);
        let ownership = crate::ownership::ResourceOwnership::Owner(
            crate::ownership::Owner::User(user_id)
        );
        context = context.with_ownership("station", ownership);

        // Should pass for owned resource
        let result = guard.check_ownership(&context, "station", "station-123");
        assert!(result.is_ok());

        // Should fail for unowned resource
        let result = guard.check_ownership(&context, "charger", "charger-123");
        assert!(matches!(result, Err(AuthorizationError::OwnershipRequired)));
    }

    #[test]
    fn admin_ownership_guard_bypass() {
        let guard = AdminOwnershipGuard;
        let user_id = uuid::Uuid::new_v4();
        let roles = RoleSet::from_roles(&[Role::Admin]);
        let context = AuthorizationContext::new(user_id, roles);

        // Admin should bypass ownership checks
        let result = guard.check_ownership(&context, "station", "station-123");
        assert!(result.is_ok());
    }
}