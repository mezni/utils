use async_trait::async_trait;
use bornemap_core::AppError;
use crate::oauth::{OAuthProfile, OAuthTokenBundle};

#[async_trait]
pub trait OAuthProvider: Send + Sync {
    fn provider_name(&self) -> &'static str;

    fn authorization_url(&self, state: &str, redirect_uri: &str) -> String;

    async fn exchange_code(&self, code: String, redirect_uri: &str) -> Result<OAuthTokenBundle, AppError>;

    async fn fetch_profile(&self, tokens: &OAuthTokenBundle) -> Result<OAuthProfile, AppError>;

    fn supports_pkce(&self) -> bool {
        false
    }

    fn scopes(&self) -> Vec<String> {
        vec!["openid".to_string(), "profile".to_string(), "email".to_string()]
    }
}