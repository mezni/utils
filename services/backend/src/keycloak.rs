use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub struct KeycloakUser {
    pub username: String,
    pub email: String,
    pub enabled: bool,
    pub email_verified: bool,
    pub realm_roles: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct KeycloakUserResponse {
    pub id: String,
    pub username: String,
    pub email: Option<String>,
}

pub struct KeycloakClient {
    client: Client,
    base_url: String,
    admin_username: String,
    admin_password: String,
    realm: String,
}

impl KeycloakClient {
    pub fn new(base_url: &str, admin_username: &str, admin_password: &str, realm: &str) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
            admin_username: admin_username.to_string(),
            admin_password: admin_password.to_string(),
            realm: realm.to_string(),
        }
    }

    async fn get_admin_token(&self) -> Result<String, Box<dyn std::error::Error>> {
        let resp = self
            .client
            .post(format!("{}/realms/master/protocol/openid-connect/token", self.base_url))
            .form(&[
                ("username", &self.admin_username),
                ("password", &self.admin_password),
                ("grant_type", "password"),
                ("client_id", "admin-cli"),
            ])
            .send()
            .await?;

        let token_resp: serde_json::Value = resp.json().await?;
        let access_token = token_resp["access_token"]
            .as_str()
            .ok_or("Failed to get admin token")?
            .to_string();

        Ok(access_token)
    }

    pub async fn create_user(&self, user: &KeycloakUser) -> Result<String, Box<dyn std::error::Error>> {
        let token = self.get_admin_token().await?;

        let payload = serde_json::json!({
            "username": user.username,
            "email": user.email,
            "enabled": user.enabled,
            "emailVerified": user.email_verified,
            "realmRoles": user.realm_roles,
        });

        let resp = self
            .client
            .post(format!("{}/admin/realms/{}/users", self.base_url, self.realm))
            .bearer_auth(&token)
            .json(&payload)
            .send()
            .await?;

        if !resp.status().is_success() {
            let body = resp.text().await?;
            return Err(format!("Keycloak API error: {}", body).into());
        }

        // Get user ID from Location header
        let location = resp
            .headers()
            .get("Location")
            .ok_or("No Location header in response")?;

        let user_id = location
            .to_str()?
            .split('/')
            .last()
            .ok_or("Failed to parse user ID")?
            .to_string();

        Ok(user_id)
    }

    pub async fn assign_realm_roles(
        &self,
        user_id: &str,
        roles: &[&str],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let token = self.get_admin_token().await?;

        let role_mappings: Vec<_> = roles
            .iter()
            .map(|r| serde_json::json!({ "name": r, "id": r }))
            .collect();

        let resp = self
            .client
            .post(format!(
                "{}/admin/realms/{}/users/{}/role-mappings/realm",
                self.base_url, self.realm, user_id
            ))
            .bearer_auth(&token)
            .json(&role_mappings)
            .send()
            .await?;

        if !resp.status().is_success() {
            let body = resp.text().await?;
            return Err(format!("Failed to assign roles: {}", body).into());
        }

        Ok(())
    }
}
