use crate::CurrentUser;
use common_types::Role;
use tracing::info;

/// Placeholder for the platform_db user_account table operations.
/// In Sprint 4 the actual DB schema will be available; for now we
/// use an in-memory stub that logs provisioning events.
///
/// On first valid JWT login:
///   1. Look up user_account by keycloak_user_id
///   2. If not found, create one with USR- ULID
///   3. If user has partner_id attribute, create partner_membership
///   4. Return CurrentUser

#[derive(Debug, Clone)]
pub struct ProvisionedUser {
    pub user_id: String,
    pub keycloak_user_id: String,
    pub email: Option<String>,
    pub role: Role,
    pub partner_id: Option<String>,
}

/// Attempt to provision a user on first login.
/// Returns CurrentUser ready for the auth layer.
pub async fn provision_user(
    keycloak_user_id: &str,
    email: Option<&str>,
    role: Role,
) -> ProvisionedUser {
    // TODO(Sprint 4): Replace with actual platform_db query/insert.
    // For now, generate a deterministic stub user_id.
    let user_id = format!("USR-{}", &keycloak_user_id[..8].to_uppercase());

    info!(
        user_id = %user_id,
        keycloak_user_id = %keycloak_user_id,
        role = ?role,
        "User provisioned"
    );

    ProvisionedUser {
        user_id,
        keycloak_user_id: keycloak_user_id.to_string(),
        email: email.map(|e| e.to_string()),
        role,
        partner_id: None,
    }
}

/// Return a CurrentUser from a ProvisionedUser.
impl From<ProvisionedUser> for CurrentUser {
    fn from(p: ProvisionedUser) -> Self {
        CurrentUser {
            user_id: p.user_id,
            keycloak_user_id: p.keycloak_user_id,
            email: p.email,
            role: p.role,
            partner_id: p.partner_id,
        }
    }
}
