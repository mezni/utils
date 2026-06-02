use crate::CurrentUser;
use common_types::Role;
use tracing::info;

/// Placeholder for the platform_db user_account table operations.
/// In Sprint 4 the actual DB schema will be available; for now we
/// use an in-memory stub that logs provisioning events.
///
/// On first valid JWT login:
///   1. Look up user_account by keycloak_user_id
///   2. If not found, create one with a USR- prefixed identifier
///   3. If user has partner_id attribute, create partner_membership
///      and derive `partner_id` from membership (NEVER from the client)
///   4. Return CurrentUser

#[derive(Debug, Clone)]
pub struct ProvisionedUser {
    pub user_id: String,
    pub keycloak_user_id: String,
    pub email: Option<String>,
    pub role: Role,
    pub partner_id: Option<String>,
}

/// Derive a stable, collision-resistant `USR-` identifier from the Keycloak `sub`.
///
/// This is a deterministic stub for the pre-DB sprint. It is byte-safe (no slicing of
/// arbitrary UTF-8) and produces a fixed-length Crockford-base32-style suffix. Sprint 4
/// replaces this with a persisted ULID allocated at first INSERT.
fn derive_user_id(keycloak_user_id: &str) -> String {
    // FNV-1a 64-bit hash — dependency-free and stable across runs.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in keycloak_user_id.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }

    const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
    let mut suffix = [b'0'; 13];
    let mut value = hash;
    for slot in suffix.iter_mut().rev() {
        *slot = ALPHABET[(value & 0x1f) as usize];
        value >>= 5;
    }
    // Safe: ALPHABET is ASCII so the suffix is valid UTF-8.
    format!("USR-{}", std::str::from_utf8(&suffix).unwrap())
}

/// Attempt to provision a user on first login.
/// Returns a `ProvisionedUser` ready for the auth layer.
pub async fn provision_user(
    keycloak_user_id: &str,
    email: Option<&str>,
    role: Role,
) -> ProvisionedUser {
    // TODO(Sprint 4): Replace with actual platform_db SELECT-then-INSERT.
    //   - Look up user_account by keycloak_user_id; allocate a real ULID on insert.
    //   - Derive partner_id from partner_membership (never accept it from the client).
    let user_id = derive_user_id(keycloak_user_id);

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
        // partner_id is ALWAYS derived from partner_membership (Sprint 4), NEVER from
        // the client/token. Until the membership table exists it remains None.
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
