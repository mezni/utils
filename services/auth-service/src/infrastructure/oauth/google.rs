use async_trait::async_trait;
use bornemap_core::AppError;
use bornemap_auth::{OAuthProfile, OAuthProvider, OAuthTokenBundle};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use reqwest::Client;

#[derive(Debug, Clone)]
pub struct GoogleOAuthProvider {
    client_id: String,
    client_secret: String,
    redirect_uri: String,
    auth_url: String,
    token_url: String,
    userinfo_url: String,
    http_client: Client,
}

#[derive(Debug, Deserialize)]
struct GoogleTokenResponse {
    access_token: String,
    id_token: Option<String>,
    refresh_token: Option<String>,
    expires_in: i64,
    token_type: String,
}

#[derive(Debug, Deserialize)]
struct GoogleUserInfoResponse {
    sub: String,
    email: String,
    email_verified: bool,
    name: Option<String>,
    given_name: Option<String>,
    family_name: Option<String>,
    picture: Option<String>,
}

impl GoogleOAuthProvider {
    pub fn new(
        client_id: String,
        client_secret: String,
        redirect_uri: String,
        auth_url: String,
        token_url: String,
        userinfo_url: String,
    ) -> Self {
        Self {
            client_id,
            client_secret,
            redirect_uri,
            auth_url,
            token_url,
            userinfo_url,
            http_client: Client::new(),
        }
    }

    fn build_scopes(&self) -> String {
        self.scopes().join(" ")
    }

    async fn exchange_code_for_tokens(&self, code: &str) -> Result<GoogleTokenResponse, AppError> {
        let params = [
            ("client_id", &self.client_id),
            ("client_secret", &self.client_secret),
            ("code", code),
            ("grant_type", "authorization_code"),
            ("redirect_uri", &self.redirect_uri),
        ];

        let response = self.http_client
            .post(&self.token_url)
            .form(&params)
            .send()
            .await
            .map_err(|e| AppError::OAuthProviderUnavailable(e.to_string()))?;

        if !response.status().is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(AppError::OAuthTokenExchangeFailed(error_text));
        }

        let token_response: GoogleTokenResponse = response
            .json()
            .await
            .map_err(|e| AppError::OAuthTokenExchangeFailed(e.to_string()))?;

        Ok(token_response)
    }

    async fn fetch_user_info(&self, access_token: &str) -> Result<GoogleUserInfoResponse, AppError> {
        let response = self.http_client
            .get(&self.userinfo_url)
            .header("Authorization", format!("Bearer {}", access_token))
            .send()
            .await
            .map_err(|e| AppError::OAuthProfileFetchFailed(e.to_string()))?;

        if !response.status().is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(AppError::OAuthProfileFetchFailed(error_text));
        }

        let user_info: GoogleUserInfoResponse = response
            .json()
            .await
            .map_err(|e| AppError::OAuthProfileFetchFailed(e.to_string()))?;

        Ok(user_info)
    }
}

#[async_trait]
impl OAuthProvider for GoogleOAuthProvider {
    fn provider_name(&self) -> &'static str {
        "google"
    }

    fn authorization_url(&self, state: &str, redirect_uri: &str) -> String {
        let params = [
            ("client_id", &self.client_id),
            ("redirect_uri", redirect_uri),
            ("response_type", "code"),
            ("scope", &self.build_scopes()),
            ("state", state),
        ];

        let mut url = self.auth_url.clone();
        let separator = if url.contains('?') { '&' } else { '?' };
        url.push_str(separator);
        url.push_str(&serde_urlencoded::to_string(params).unwrap());
        
        url
    }

    async fn exchange_code(&self, code: String, _redirect_uri: &str) -> Result<OAuthTokenBundle, AppError> {
        let token_response = self.exchange_code_for_tokens(&code).await?;

        Ok(OAuthTokenBundle {
            access_token: token_response.access_token,
            id_token: token_response.id_token,
            refresh_token: token_response.refresh_token,
        })
    }

    async fn fetch_profile(&self, tokens: &OAuthTokenBundle) -> Result<OAuthProfile, AppError> {
        let user_info = self.fetch_user_info(&tokens.access_token).await?;

        // Check if email is verified
        if !user_info.email_verified {
            return Err(AppError::OAuthEmailNotVerified);
        }

        // Parse name into first and last name
        let (first_name, last_name) = if let Some(full_name) = user_info.name {
            let parts: Vec<&str> = full_name.split_whitespace().collect();
            match parts.len() {
                0 => (None, None),
                1 => (Some(parts[0].to_string()), None),
                _ => (Some(parts[0].to_string()), Some(parts[1..].join(" "))),
            }
        } else {
            (user_info.given_name, user_info.family_name)
        };

        let mut raw_attributes = HashMap::new();
        raw_attributes.insert("sub".to_string(), serde_json::Value::String(user_info.sub.clone()));
        raw_attributes.insert("email".to_string(), serde_json::Value::String(user_info.email.clone()));
        raw_attributes.insert("email_verified".to_string(), serde_json::Value::Bool(user_info.email_verified));
        if let Some(name) = user_info.name {
            raw_attributes.insert("name".to_string(), serde_json::Value::String(name));
        }
        if let Some(given_name) = user_info.given_name {
            raw_attributes.insert("given_name".to_string(), serde_json::Value::String(given_name));
        }
        if let Some(family_name) = user_info.family_name {
            raw_attributes.insert("family_name".to_string(), serde_json::Value::String(family_name));
        }
        if let Some(picture) = user_info.picture {
            raw_attributes.insert("picture".to_string(), serde_json::Value::String(picture));
        }

        Ok(OAuthProfile::new(
            user_info.sub,
            user_info.email,
            user_info.email_verified,
            self.provider_name().to_string(),
        )
        .with_name(first_name, last_name)
        .with_avatar(user_info.picture)
        .with_raw_attributes(raw_attributes))
    }

    fn scopes(&self) -> Vec<String> {
        vec![
            "openid".to_string(),
            "profile".to_string(),
            "email".to_string(),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_authorization_url_construction() {
        let provider = GoogleOAuthProvider::new(
            "test_client_id".to_string(),
            "test_client_secret".to_string(),
            "http://localhost:8080/callback".to_string(),
            "https://accounts.google.com/o/oauth2/v2/auth".to_string(),
            "https://oauth2.googleapis.com/token".to_string(),
            "https://openidconnect.googleapis.com/v1/userinfo".to_string(),
        );

        let state = "test-state-123";
        let redirect_uri = "http://localhost:8080/callback";
        let url = provider.authorization_url(state, redirect_uri);

        assert!(url.contains("client_id=test_client_id"));
        assert!(url.contains("redirect_uri=http://localhost:8080/callback"));
        assert!(url.contains("response_type=code"));
        assert!(url.contains("scope=openid+profile+email"));
        assert!(url.contains("state=test-state-123"));
        assert!(url.starts_with("https://accounts.google.com/o/oauth2/v2/auth"));
    }

    #[test]
    fn test_scopes() {
        let provider = GoogleOAuthProvider::new(
            "test_client_id".to_string(),
            "test_client_secret".to_string(),
            "http://localhost:8080/callback".to_string(),
            "https://accounts.google.com/o/oauth2/v2/auth".to_string(),
            "https://oauth2.googleapis.com/token".to_string(),
            "https://openidconnect.googleapis.com/v1/userinfo".to_string(),
        );

        let scopes = provider.scopes();
        assert_eq!(scopes, vec!["openid", "profile", "email"]);
    }
}