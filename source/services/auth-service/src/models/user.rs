use serde::{Deserialize, Serialize};

/// User profile stored in the platform database.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UserProfile {
    pub id: String,
    pub keycloak_sub: String,
    pub email: String,
    pub display_name: String,
    pub roles: Vec<String>,
    pub last_login_at: chrono::DateTime<chrono::Utc>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// SQL query for upserting a user profile.
///
/// This uses an ON CONFLICT to update the row if it exists.
pub struct UpsertUser<'a> {
    pub id: &'a str,
    pub keycloak_sub: &'a str,
    pub email: &'a str,
    pub display_name: &'a str,
    pub roles: &'a [String],
    pub last_login_at: chrono::DateTime<chrono::Utc>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl<'a> UpsertUser<'a> {
    /// Execute the upsert query against a sqlx pool.
    pub async fn execute(self, pool: &sqlx::PgPool) -> sqlx::Result<sqlx::postgres::PgQueryResult> {
        sqlx::query(
            r#"
            INSERT INTO users.user_profiles (
                id, keycloak_sub, email, display_name, roles,
                last_login_at, created_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (keycloak_sub)
            DO UPDATE SET
                email = EXCLUDED.email,
                display_name = EXCLUDED.display_name,
                roles = EXCLUDED.roles,
                last_login_at = EXCLUDED.last_login_at,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(self.id)
        .bind(self.keycloak_sub)
        .bind(self.email)
        .bind(self.display_name)
        .bind(self.roles)
        .bind(self.last_login_at)
        .bind(self.created_at)
        .bind(self.updated_at)
        .execute(pool)
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_profile_serialization() {
        let now = chrono::Utc::now();
        let profile = UserProfile {
            id: "USR-abc123".to_string(),
            keycloak_sub: "keycloak_sub_123".to_string(),
            email: "test@example.com".to_string(),
            display_name: "Test User".to_string(),
            roles: vec!["role:admin".to_string()],
            last_login_at: now,
            created_at: now,
            updated_at: now,
        };

        let json = serde_json::to_string(&profile).unwrap();
        assert!(json.contains("\"id\""));
        assert!(json.contains("\"USR-abc123\""));
    }

    #[test]
    fn test_upsert_query_binds() {
        let upsert = UpsertUser {
            id: "USR-abc123",
            keycloak_sub: "keycloak_sub_123",
            email: "test@example.com",
            display_name: "Test User",
            roles: &["role:admin".to_string()],
            last_login_at: chrono::Utc::now(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        // Just verify the struct can be created
        assert_eq!(upsert.id, "USR-abc123");
        assert_eq!(upsert.keycloak_sub, "keycloak_sub_123");
        assert_eq!(upsert.email, "test@example.com");
    }
}
