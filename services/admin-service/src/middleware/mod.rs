//! Middleware module for admin-service

pub mod auth;
pub mod role_auth;
pub mod partner_isolation;

pub use auth::{KeycloakAuth, UserClaims, AuthUser, KeycloakConfig};
pub use role_auth::{RoleAuth, RoleConfig, AnalyticsRoleConfig};
pub use partner_isolation::{PartnerIsolation, PartnerIsolationContext, PartnerIsolationError};