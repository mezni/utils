use async_trait::async_trait;
use bornemap_core::AppError;

pub mod google;

#[async_trait]
pub trait OAuthProvider: Send + Sync {
    fn authorization_url(&self, state: &str, redirect_uri: &str) -> String;
    async fn exchange_code_for_tokens(&self, code: &str) -> Result<OAuthTokenBundle, AppError>;
    async fn get_user_profile(&self, access_token: &str) -> Result<OAuthProfile, AppError>;
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct OAuthTokenBundle {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: u64,
    pub refresh_token: Option<String>,
    pub scope: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct OAuthProfile {
    pub sub: String,
    pub email: String,
    pub email_verified: bool,
    pub name: Option<String>,
    pub given_name: Option<String>,
    pub family_name: Option<String>,
    pub picture: Option<String>,
    pub raw_attributes: std::collections::HashMap<String, serde_json::Value>,
}

impl OAuthProfile {
    pub fn new(sub: String, email: String, email_verified: bool) -> Self {
        Self {
            sub,
            email,
            email_verified,
            name: None,
            given_name: None,
            family_name: None,
            picture: None,
            raw_attributes: std::collections::HashMap::new(),
        }
    }
}