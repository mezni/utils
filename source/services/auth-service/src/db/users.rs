use crate::error::AuthError;
use crate::models::user::{UserProfile, UpsertUser};
use crate::validation::token::validate_required;
use chrono::Utc;
use uuid::Uuid;

/// Repository for user operations in the platform database.
pub struct UsersRepository;

impl UsersRepository {
    /// Create a new repository instance.
    pub fn new() -> Self {
        Self
    }

    /// Upsert a user profile in the database.
    ///
    /// If the user already exists (by keycloak_sub), their profile is updated.
    /// Otherwise, a new profile is created.
    pub async fn upsert_user(
        &self,
        pool: &PgPool,
        claims: &Claims,
    ) -> Result<UserProfile, AuthError> {
        validate_required("sub", &claims.sub)?;
        validate_required("email", &claims.email)?;

        tracing::info!(
            "Upserting user profile: sub={}, email={}",
            claims.sub,
            claims.email
        );

        let id = Self::generate_user_id(&claims.sub);
        let display_name = claims.display_name();
        let roles = claims.known_roles();

        let now = Utc::now();
        let upsert = UpsertUser {
            id: &id,
            keycloak_sub: &claims.sub,
            email: &claims.email,
            display_name: &display_name,
            roles: &roles,
            last_login_at: now,
            created_at: now,
            updated_at: now,
        };

        upsert.execute(pool).await.map_err(|e| {
            tracing::error!("Failed to upsert user profile: {}", e);
            AuthError::AuthUnavailable
        })?;

        Ok(UserProfile {
            id,
            keycloak_sub: claims.sub.clone(),
            email: claims.email.clone(),
            display_name,
            roles,
            last_login_at: now,
            created_at: now,
            updated_at: now,
        })
    }

    /// Generate a user ID from the Keycloak sub claim.
    ///
    /// Uses UUIDv5 for deterministic uniqueness based on the OIDC namespace.
    fn generate_user_id(sub: &str) -> String {
        let uuid = Uuid::new_v5(&Uuid::NAMESPACE_OID, sub.as_bytes());
        format!("USR-{}", uuid.to_string())
    }

    /// Fetch a user profile by their Keycloak sub.
    pub async fn get_user_by_sub(&self, pool: &PgPool, keycloak_sub: &str) -> Result<UserProfile, AuthError> {
        validate_required("keycloak_sub", keycloak_sub)?;

        let profile = sqlx::query_as!(
            UserProfile,
            r#"
            SELECT
                id,
                keycloak_sub,
                email,
                display_name,
                roles,
                last_login_at,
                created_at,
                updated_at
            FROM users.user_profiles
            WHERE keycloak_sub = $1
            "#,
            keycloak_sub
        )
        .fetch_one(pool)
        .await
        .map_err(|e| {
            tracing::error!("Failed to fetch user: {}", e);
            AuthError::ValidationError("User not found".to_string())
        })?;

        Ok(profile)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use claims::Claims;

    #[test]
    fn test_generate_user_id() {
        let id = UsersRepository::generate_user_id("test_sub_123");
        assert!(id.starts_with("USR-"));
        assert!(id.len() == 28); // USR- + UUID (32 chars)
    }

    #[test]
    fn test_generate_user_id_consistent() {
        let id1 = UsersRepository::generate_user_id("test_sub_123");
        let id2 = UsersRepository::generate_user_id("test_sub_123");
        assert_eq!(id1, id2);
    }
}
