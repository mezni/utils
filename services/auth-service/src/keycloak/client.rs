use reqwest::Client;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::config::AppConfig;

#[derive(Debug, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub expires_in: u64,
    pub token_type: String,
}

#[derive(Debug, Deserialize)]
pub struct UserRepresentation {
    pub id: Option<String>,
    pub username: Option<String>,
    pub email: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub enabled: Option<bool>,
    pub email_verified: Option<bool>,
    pub realm_roles: Option<Vec<String>>,
    pub attributes: Option<std::collections::HashMap<String, Vec<String>>>,
}

#[derive(Debug, Deserialize)]
pub struct RoleRepresentation {
    pub id: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub composite: Option<bool>,
    pub client_role: Option<bool>,
    pub container_id: Option<String>,
}

pub struct KeycloakAdminClient {
    client: Client,
    config: AppConfig,
}

impl KeycloakAdminClient {
    pub fn new(config: AppConfig) -> Self {
        Self {
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("Failed to create HTTP client"),
            config,
        }
    }

    pub async fn get_admin_token(&self) -> Result<String, String> {
        let params = [
            ("client_id", "admin-cli"),
            ("username", &self.config.keycloak_admin_username),
            ("password", &self.config.keycloak_admin_password),
            ("grant_type", "password"),
        ];

        let resp = self
            .client
            .post(format!(
                "{}/realms/master/protocol/openid-connect/token",
                self.config.keycloak_url
            ))
            .form(&params)
            .send()
            .await
            .map_err(|e| format!("Keycloak token request failed: {}", e))?;

        let token: TokenResponse = resp
            .json()
            .await
            .map_err(|e| format!("Keycloak token parse failed: {}", e))?;

        Ok(token.access_token)
    }

    pub async fn get_user(&self, user_id: &Uuid) -> Result<Option<UserRepresentation>, String> {
        let token = self.get_admin_token().await?;

        let resp = self
            .client
            .get(format!(
                "{}/admin/realms/{}/users/{}",
                self.config.keycloak_url, self.config.keycloak_realm, user_id
            ))
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|e| format!("Keycloak user lookup failed: {}", e))?;

        if resp.status().is_success() {
            let user: UserRepresentation = resp
                .json()
                .await
                .map_err(|e| format!("Failed to parse user: {}", e))?;
            Ok(Some(user))
        } else if resp.status().as_u16() == 404 {
            Ok(None)
        } else {
            Err(format!(
                "Keycloak API error: {}",
                resp.status()
            ))
        }
    }

    pub async fn get_user_by_username(
        &self,
        username: &str,
    ) -> Result<Option<UserRepresentation>, String> {
        let token = self.get_admin_token().await?;

        let resp = self
            .client
            .get(format!(
                "{}/admin/realms/{}/users?username={}&exact=true",
                self.config.keycloak_url, self.config.keycloak_realm, username
            ))
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|e| format!("Keycloak user search failed: {}", e))?;

        let users: Vec<UserRepresentation> = resp
            .json()
            .await
            .map_err(|e| format!("Failed to parse users: {}", e))?;

        Ok(users.into_iter().next())
    }

    pub async fn get_realm_roles(&self) -> Result<Vec<RoleRepresentation>, String> {
        let token = self.get_admin_token().await?;

        let resp = self
            .client
            .get(format!(
                "{}/admin/realms/{}/roles",
                self.config.keycloak_url, self.config.keycloak_realm
            ))
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|e| format!("Keycloak roles fetch failed: {}", e))?;

        let roles: Vec<RoleRepresentation> = resp
            .json()
            .await
            .map_err(|e| format!("Failed to parse roles: {}", e))?;

        Ok(roles)
    }
}
