use serde::Deserialize;

/// JWT claims extracted from a Keycloak token response.
#[derive(Debug, Deserialize, Clone)]
pub struct Claims {
    pub sub: String,
    pub email: String,
    #[serde(rename = "given_name")]
    pub given_name: Option<String>,
    #[serde(rename = "family_name")]
    pub family_name: Option<String>,
    #[serde(rename = "realm_access")]
    pub realm_access: Option<RealmAccess>,
    #[serde(rename = "aud")]
    pub aud: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RealmAccess {
    pub roles: Vec<String>,
}

impl Claims {
    /// Build a display name from given_name and family_name.
    pub fn display_name(&self) -> String {
        match (self.given_name.as_deref(), self.family_name.as_deref()) {
            (Some(given), Some(family)) => format!("{} {}", given, family),
            (Some(given), None) => given.to_string(),
            (None, Some(family)) => family.to_string(),
            (None, None) => self.email.clone(),
        }
    }

    /// Filter roles to only include known roles.
    pub fn known_roles(&self) -> Vec<String> {
        let known_roles = vec!["role:admin", "role:partner", "role:driver"];
        self.realm_access
            .as_ref()
            .map(|ra| ra.roles.iter().filter(|r| known_roles.contains(r)).cloned().collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_name_full_name() {
        let claims = Claims {
            sub: "123".to_string(),
            email: "test@example.com".to_string(),
            given_name: Some("John".to_string()),
            family_name: Some("Doe".to_string()),
            realm_access: None,
            aud: vec!["bornemap".to_string()],
        };

        assert_eq!(claims.display_name(), "John Doe");
    }

    #[test]
    fn test_display_name_given_only() {
        let claims = Claims {
            sub: "123".to_string(),
            email: "test@example.com".to_string(),
            given_name: Some("Jane".to_string()),
            family_name: None,
            realm_access: None,
            aud: vec!["bornemap".to_string()],
        };

        assert_eq!(claims.display_name(), "Jane");
    }

    #[test]
    fn test_known_roles_filtering() {
        let claims = Claims {
            sub: "123".to_string(),
            email: "test@example.com".to_string(),
            given_name: None,
            family_name: None,
            realm_access: Some(RealmAccess {
                roles: vec![
                    "role:admin".to_string(),
                    "unknown_role".to_string(),
                    "role:partner".to_string(),
                    "role:driver".to_string(),
                ],
            }),
            aud: vec!["bornemap".to_string()],
        };

        let roles = claims.known_roles();
        assert_eq!(roles.len(), 3);
        assert!(roles.contains(&"role:admin".to_string()));
        assert!(roles.contains(&"role:partner".to_string()));
        assert!(roles.contains(&"role:driver".to_string()));
        assert!(!roles.contains(&"unknown_role".to_string()));
    }

    #[test]
    fn test_known_roles_no_access() {
        let claims = Claims {
            sub: "123".to_string(),
            email: "test@example.com".to_string(),
            given_name: None,
            family_name: None,
            realm_access: None,
            aud: vec!["bornemap".to_string()],
        };

        let roles = claims.known_roles();
        assert_eq!(roles.len(), 0);
    }
}
