# Identity Core Library Contract

**Crate**: `identity-core` (`source/services/libs/identity-core`)

**Purpose**: Shared JWT validation, claim extraction, and Keycloak Admin API client for all services.

---

## Public Interface

### JwtValidator

```rust
pub struct JwtValidator {
    // ...
}

impl JwtValidator {
    /// Create a new validator that fetches JWKS from the given Keycloak issuer URL.
    /// JWKS is cached and refreshed every 5 minutes (or on validation failure).
    pub fn new(issuer_url: &str, jwks_url: &str) -> Self;

    /// Validate an access token and extract identity claims.
    /// Returns Ok(IdentityClaims) on success, or a JwtError on failure.
    pub async fn validate_token(&self, token: &str) -> Result<IdentityClaims, JwtError>;

    /// Force refresh the JWKS cache immediately.
    pub async fn refresh_jwks(&self) -> Result<(), JwtError>;
}
```

### IdentityClaims

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityClaims {
    pub sub: String,            // Keycloak user UUID
    pub email: String,
    pub usr_id: String,         // Platform identifier (USR-...)
    pub realm: String,          // bm-drivers | bm-control
    pub roles: Vec<String>,     // ["registered_driver"] | ["admin"] | ["partner"]
    pub status: String,         // ACTIVE | DISABLED
    pub exp: usize,
    pub iat: usize,
}
```

### JwtError

```rust
#[derive(Debug, thiserror::Error)]
pub enum JwtError {
    #[error("Token is expired")]
    TokenExpired,
    #[error("Token signature is invalid")]
    InvalidSignature,
    #[error("Token issuer does not match")]
    InvalidIssuer,
    #[error("Token audience does not match")]
    InvalidAudience,
    #[error("Token missing required claim: {0}")]
    MissingClaim(&'static str),
    #[error("JWKS fetch failed: {0}")]
    JwksFetchFailed(String),
    #[error("Internal error: {0}")]
    Internal(String),
}
```

### KeycloakAdminClient

```rust
pub struct KeycloakAdminClient {
    // ...
}

impl KeycloakAdminClient {
    /// Create a new admin client. Authenticates with service account credentials.
    pub fn new(server_url: &str, realm: &str, client_id: &str, client_secret: &str) -> Self;

    /// Create a user in the specified realm. Returns the Keycloak user UUID.
    pub async fn create_user(
        &self,
        email: &str,
        password: &str,
        first_name: Option<&str>,
        last_name: Option<&str>,
    ) -> Result<String, AdminError>;

    /// Assign a realm role to a user.
    pub async fn assign_role(
        &self,
        user_id: &str,
        role_name: &str,
    ) -> Result<(), AdminError>;

    /// Get a user's Keycloak ID by email.
    pub async fn get_user_by_email(&self, email: &str) -> Result<Option<UserInfo>, AdminError>;

    /// Disable or enable a user account in Keycloak.
    pub async fn set_user_enabled(&self, user_id: &str, enabled: bool) -> Result<(), AdminError>;

    /// Trigger Keycloak logout for a user (invalidates all sessions).
    pub async fn logout_user(&self, user_id: &str) -> Result<(), AdminError>;
}
```

### AuthMiddleware (for actix-web)

```rust
/// Actix-web middleware that extracts and validates JWT from Authorization header.
/// Injects IdentityClaims into request extensions.
pub struct AuthMiddleware;

impl AuthMiddleware {
    pub fn new(validator: JwtValidator) -> Self;
}

/// Extract IdentityClaims from request extensions.
pub struct AuthenticatedUser(pub IdentityClaims);

impl actix_web::FromRequest for AuthenticatedUser { ... }
```
