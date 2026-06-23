//! Partner isolation filter middleware
//! Enforces partner isolation for manager users

use actix_web::{dev::{RequestHead, FromRequest}, http::header::AUTHORIZATION, Error};;
use std::sync::Arc;

/// Partner isolation context
#[derive(Clone)]
pub struct PartnerIsolationContext {
    /// Current partner ID (None for admin or no filter)
    pub current_partner_id: Option<String>,
    /// Filter results by partner ID if set
    pub filter_by_partner: bool,
    /// Require partner_id in query (for manager users)
    pub require_partner_id: bool,
}

impl PartnerIsolationContext {
    /// Create new context
    pub fn new(
        current_partner_id: Option<String>,
        filter_by_partner: bool,
        require_partner_id: bool,
    ) -> Self {
        Self {
            current_partner_id,
            filter_by_partner,
            require_partner_id,
        }
    }

    /// Check if user is admin (no partner restriction)
    pub fn is_admin(&self) -> bool {
        self.current_partner_id.is_none()
    }

    /// Check if user is manager (partner-restricted access)
    pub fn is_manager(&self) -> bool {
        self.current_partner_id.is_some()
    }

    /// Get effective partner_id
    pub fn effective_partner_id(&self) -> Option<&str> {
        self.current_partner_id.as_deref()
    }
}

impl Default for PartnerIsolationContext {
    fn default() -> Self {
        Self {
            current_partner_id: None,
            filter_by_partner: true,
            require_partner_id: false,
        }
    }
}

/// Partner isolation middleware
pub struct PartnerIsolation;

impl PartnerIsolation {
    /// Enforce partner isolation for manager users
    pub fn enforce(
        partner_id: &str,
        required_partner_id: Option<&str>,
    ) -> Result<(), PartnerIsolationError> {
        // If required_partner_id is provided, verify it matches
        if let Some(expected) = required_partner_id {
            if partner_id != expected {
                return Err(PartnerIsolationError::PartnerMismatch {
                    current: partner_id.to_string(),
                    expected: expected.to_string(),
                });
            }
        }

        Ok(())
    }

    /// Validate partner ID format
    pub fn validate_partner_id(partner_id: &str) -> Result<(), PartnerIsolationError> {
        // PREFIX-nanoid(12) format: 3 uppercase letters + "-" + 12 characters
        let pattern = r"^[A-Z]{3}-[A-Za-z0-9]{12}$";

        if !regex::Regex::new(pattern).unwrap().is_match(partner_id) {
            return Err(PartnerIsolationError::InvalidFormat {
                partner_id: partner_id.to_string(),
            });
        }

        Ok(())
    }

    /// Create context for analytics endpoint
    pub fn create_analytics_context(
        partner_id: Option<String>,
        require_partner_id: bool,
    ) -> PartnerIsolationContext {
        PartnerIsolationContext {
            current_partner_id: partner_id,
            filter_by_partner: true,
            require_partner_id,
        }
    }
}

impl FromRequest for PartnerIsolationContext {
    type Error = Error;
    type Future = std::future::Ready<Result<Self, Self::Error>>;

    fn from_request(req: &mut RequestHead, _payload: &mut actix_web::dev::Payload) -> Self::Future {
        // Extract partner_id from query parameters or context
        // This would typically come from the authentication middleware
        let partner_id = None; // Default: no partner restriction

        std::future::ready(Ok(PartnerIsolationContext {
            current_partner_id: partner_id,
            filter_by_partner: true,
            require_partner_id: false,
        }))
    }
}

/// Partner isolation errors
#[derive(Debug, thiserror::Error)]
pub enum PartnerIsolationError {
    #[error("Partner ID mismatch: current={0}, expected={1}")]
    PartnerMismatch {
        current: String,
        expected: String,
    },
    #[error("Invalid partner ID format: {0}")]
    InvalidFormat { partner_id: String },
    #[error("Partner ID required for manager access")]
    MissingPartnerId,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_is_admin() {
        let context = PartnerIsolationContext::new(None, true, false);
        assert!(context.is_admin());
        assert!(!context.is_manager());
    }

    #[test]
    fn test_context_is_manager() {
        let context = PartnerIsolationContext::new(Some("STX-xxx".to_string()), true, false);
        assert!(!context.is_admin());
        assert!(context.is_manager());
    }

    #[test]
    fn test_partner_id_validation_valid() {
        assert!(PartnerIsolation::validate_partner_id("STX-abc123def456").is_ok());
        assert!(PartnerIsolation::validate_partner_id("OPS-xyz789def456").is_ok());
    }

    #[test]
    fn test_partner_id_validation_invalid() {
        assert!(PartnerIsolation::validate_partner_id("STX-abc").is_err());
        assert!(PartnerIsolation::validate_partner_id("STA-123456789012").is_err());
        assert!(PartnerIsolation::validate_partner_id("invalid-123").is_err());
    }

    #[test]
    fn test_enforce_partner_isolation_no_filter() {
        let result = PartnerIsolation::enforce("STA-xxx", None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_enforce_partner_isolation_with_filter() {
        let result = PartnerIsolation::enforce("STA-xxx", Some("STA-xxx"));
        assert!(result.is_ok());

        let result = PartnerIsolation::enforce("STA-xxx", Some("STX-yyy"));
        assert!(result.is_err());
    }
}