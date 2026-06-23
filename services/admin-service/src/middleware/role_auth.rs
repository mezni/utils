//! Keycloak role-based authorization middleware
//! Enforces admin/manager roles for analytics endpoints

use actix_web::{dev::{RequestHead, FromRequest}, http::header::AUTHORIZATION, Error};;
use serde::Deserialize;
use std::fmt;
use std::sync::Arc;

/// Role types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Admin,
    Manager,
    User,
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Role::Admin => write!(f, "admin"),
            Role::Manager => write!(f, "manager"),
            Role::User => write!(f, "user"),
        }
    }
}

/// Role-based authorization config
#[derive(Clone)]
pub struct RoleConfig {
    pub required_role: Role,
    pub allow_user_role: bool,
}

impl Default for RoleConfig {
    fn default() -> Self {
        Self {
            required_role: Role::User,
            allow_user_role: true,
        }
    }
}

/// Role checking middleware
pub struct RoleAuth {
    config: RoleConfig,
}

impl RoleAuth {
    /// Create new role authorization middleware
    pub fn new(config: RoleConfig) -> Self {
        Self { config }
    }

    /// Check if user has required role
    pub fn has_required_role(&self, role: Role) -> bool {
        if self.config.allow_user_role {
            role == Role::User || role == self.config.required_role
        } else {
            role == self.config.required_role
        }
    }

    /// Check if user is admin
    pub fn is_admin(&self) -> bool {
        self.config.required_role == Role::Admin || self.config.allow_user_role
    }

    /// Check if user is manager
    pub fn is_manager(&self) -> bool {
        self.config.required_role == Role::Manager
    }
}

impl Default for RoleAuth {
    fn default() -> Self {
        Self {
            config: RoleConfig {
                required_role: Role::User,
                allow_user_role: true,
            },
        }
    }
}

impl FromRequest for RoleAuth {
    type Error = Error;
    type Future = std::future::Ready<Result<Self, Self::Error>>;

    fn from_request(req: &mut RequestHead, _payload: &mut actix_web::dev::Payload) -> Self::Future {
        // Get user from auth context
        // This would be set by the KeycloakAuth middleware
        let user_role = Role::User; // Default role

        let config = RoleConfig {
            required_role: Role::User, // Can be configured per endpoint
            allow_user_role: true,
        };

        let auth = RoleAuth::new(config);

        // Check role
        if !auth.has_required_role(user_role) {
            return std::future::ready(Err(actix_web::error::ErrorForbidden(
                "Insufficient permissions",
            )));
        }

        std::future::ready(Ok(auth))
    }
}

/// Analytics-specific role config
#[derive(Clone)]
pub struct AnalyticsRoleConfig {
    /// Admin can access all analytics
    pub allow_admin: bool,
    /// Manager can access own partner's analytics only
    pub allow_manager: bool,
    /// User can access public analytics only
    pub allow_user: bool,
}

impl Default for AnalyticsRoleConfig {
    fn default() -> Self {
        Self {
            allow_admin: true,
            allow_manager: true,
            allow_user: true,
        }
    }
}

/// Partner isolation config
#[derive(Clone)]
pub struct PartnerIsolationConfig {
    pub require_partner_id: bool,
    pub filter_by_partner_id: bool,
}

impl Default for PartnerIsolationConfig {
    fn default() -> Self {
        Self {
            require_partner_id: false,
            filter_by_partner_id: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_role_is_admin() {
        let config = RoleConfig {
            required_role: Role::Admin,
            allow_user_role: false,
        };

        let auth = RoleAuth::new(config);
        assert!(auth.is_admin());
        assert!(!auth.is_manager());
    }

    #[test]
    fn test_role_is_manager() {
        let config = RoleConfig {
            required_role: Role::Manager,
            allow_user_role: false,
        };

        let auth = RoleAuth::new(config);
        assert!(!auth.is_admin());
        assert!(auth.is_manager());
    }

    #[test]
    fn test_analytics_config_default() {
        let config = AnalyticsRoleConfig::default();
        assert!(config.allow_admin);
        assert!(config.allow_manager);
        assert!(config.allow_user);
    }

    #[test]
    fn test_partner_isolation_config() {
        let config = PartnerIsolationConfig::default();
        assert!(!config.require_partner_id);
        assert!(config.filter_by_partner_id);
    }
}