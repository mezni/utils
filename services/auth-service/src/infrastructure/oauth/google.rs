use async_trait::async_trait;
use bornemap_core::AppError;
use std::collections::HashMap;

use super::{OAuthProfile, OAuthProvider, OAuthTokenBundle};

#[derive(serde::Deserialize)]
struct GoogleUserInfo {
    pub sub: String,
    pub email: String,
    #[serde(default)]
    pub email_verified: bool,
    pub name: Option<String>,
    pub given_name: Option<String>,
    pub family_name: Option<String>,
    pub picture: Option<String>,
}

#[derive(serde::Deserialize)]
#[allow(dead_code)]
struct GoogleTokenResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: u64,
    pub refresh_token: Option<String>,
    pub scope: Option<String>,
    pub id_token: Option<String>,
}

#[derive(Clone)]
pub struct GoogleOAuthProvider {
    client_id: String,
    client_secret: String,
    redirect_uri: String,
}

impl GoogleOAuthProvider {
    pub fn new(client_id: String, client_secret: String, redirect_uri: String) -> Self {
        Self {
            client_id,
            client_secret,
            redirect_uri,
        }
    }
}

#[async_trait]
impl OAuthProvider for GoogleOAuthProvider {
    fn authorization_url(&self, state: &str, redirect_uri: &str) -> String {
        format!(
            "https://accounts.google.com/o/oauth2/v2/auth?response_type=code&client_id={}&redirect_uri={}&scope=openid%20email%20profile&state={}",
            self.client_id, redirect_uri, state
        )
    }

    async fn exchange_code_for_tokens(&self, code: &str) -> Result<OAuthTokenBundle, AppError> {
        let client = reqwest::Client::new();
        let params = [
            ("code", code),
            ("client_id", &self.client_id),
            ("client_secret", &self.client_secret),
            ("redirect_uri", &self.redirect_uri),
            ("grant_type", "authorization_code"),
        ];

        let resp = client
            .post("https://oauth2.googleapis.com/token")
            .form(&params)
            .send()
            .await
            .map_err(|e| AppError::OAuthProviderUnavailable(e.to_string()))?;

        let token_resp: GoogleTokenResponse = resp
            .json()
            .await
            .map_err(|e| AppError::OAuthProviderUnavailable(e.to_string()))?;

        Ok(OAuthTokenBundle {
            access_token: token_resp.access_token,
            token_type: token_resp.token_type,
            expires_in: token_resp.expires_in,
            refresh_token: token_resp.refresh_token,
            scope: token_resp.scope,
        })
    }

    async fn get_user_profile(&self, access_token: &str) -> Result<OAuthProfile, AppError> {
        let client = reqwest::Client::new();
        let resp = client
            .get("https://www.googleapis.com/oauth2/v3/userinfo")
            .bearer_auth(access_token)
            .send()
            .await
            .map_err(|e| AppError::OAuthProviderUnavailable(e.to_string()))?;

        let user_info: GoogleUserInfo = resp
            .json()
            .await
            .map_err(|e| AppError::OAuthProviderUnavailable(e.to_string()))?;

        let mut raw_attributes: HashMap<String, serde_json::Value> = HashMap::new();
        raw_attributes.insert("sub".to_string(), serde_json::Value::String(user_info.sub.clone()));
        raw_attributes.insert("email".to_string(), serde_json::Value::String(user_info.email.clone()));
        raw_attributes.insert("email_verified".to_string(), serde_json::Value::Bool(user_info.email_verified));
        if let Some(name) = &user_info.name {
            raw_attributes.insert("name".to_string(), serde_json::Value::String(name.clone()));
        }
        if let Some(given_name) = &user_info.given_name {
            raw_attributes.insert("given_name".to_string(), serde_json::Value::String(given_name.clone()));
        }
        if let Some(family_name) = &user_info.family_name {
            raw_attributes.insert("family_name".to_string(), serde_json::Value::String(family_name.clone()));
        }
        if let Some(picture) = &user_info.picture {
            raw_attributes.insert("picture".to_string(), serde_json::Value::String(picture.clone()));
        }

        Ok(OAuthProfile {
            sub: user_info.sub,
            email: user_info.email,
            email_verified: user_info.email_verified,
            name: user_info.name,
            given_name: user_info.given_name,
            family_name: user_info.family_name,
            picture: user_info.picture,
            raw_attributes,
        })
    }
}
