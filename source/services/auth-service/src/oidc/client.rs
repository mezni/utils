use serde::{Deserialize, Serialize};
use url::Url;

use crate::infrastructure::config::OidcConfig;

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub id_token: Option<String>,
    pub expires_in: u64,
    pub token_type: String,
}

#[derive(Debug, Deserialize)]
struct KeycloakTokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    id_token: Option<String>,
    expires_in: u64,
    token_type: String,
}

#[derive(Debug, thiserror::Error)]
pub enum OidcError {
    #[error("HTTP request failed: {0}")]
    Http(String),

    #[error("token exchange failed: status={0}, body={1}")]
    TokenExchangeFailed(u16, String),

    #[error("invalid configuration: {0}")]
    Config(String),
}

pub struct OidcClient {
    http: reqwest::Client,
    client_id: String,
    client_secret: String,
    redirect_uri: String,
    auth_url: Url,
    token_url: Url,
    logout_url: Url,
}

impl OidcClient {
    pub fn new(config: &OidcConfig) -> Result<Self, OidcError> {
        let http = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .map_err(|e| OidcError::Http(e.to_string()))?;

        let auth_url = Url::parse(&format!(
            "{}/protocol/openid-connect/auth",
            config.issuer
        ))
        .map_err(|e| OidcError::Config(format!("invalid auth URL: {}", e)))?;

        let token_url = Url::parse(&format!(
            "{}/protocol/openid-connect/token",
            config.issuer
        ))
        .map_err(|e| OidcError::Config(format!("invalid token URL: {}", e)))?;

        let logout_url = Url::parse(&format!(
            "{}/protocol/openid-connect/logout",
            config.issuer
        ))
        .map_err(|e| OidcError::Config(format!("invalid logout URL: {}", e)))?;

        Ok(Self {
            http,
            client_id: config.client_id.clone(),
            client_secret: config.client_secret.clone(),
            redirect_uri: config.redirect_uri.clone(),
            auth_url,
            token_url,
            logout_url,
        })
    }

    pub fn build_authorize_url(&self, state: &str) -> String {
        let mut url = self.auth_url.clone();
        url.query_pairs_mut()
            .append_pair("response_type", "code")
            .append_pair("client_id", &self.client_id)
            .append_pair("redirect_uri", &self.redirect_uri)
            .append_pair("state", state)
            .append_pair("scope", "openid email profile");
        url.to_string()
    }

    pub fn build_registration_url(&self, state: &str) -> String {
        let mut url = self.auth_url.clone();
        url.query_pairs_mut()
            .append_pair("response_type", "code")
            .append_pair("client_id", &self.client_id)
            .append_pair("redirect_uri", &self.redirect_uri)
            .append_pair("state", state)
            .append_pair("scope", "openid email profile")
            .append_pair("kc_action", "register");
        url.to_string()
    }

    pub async fn exchange_code(&self, code: &str) -> Result<TokenResponse, OidcError> {
        let params = [
            ("grant_type", "authorization_code"),
            ("code", code),
            ("client_id", &self.client_id),
            ("client_secret", &self.client_secret),
            ("redirect_uri", &self.redirect_uri),
        ];

        let resp = self
            .http
            .post(self.token_url.as_str())
            .form(&params)
            .send()
            .await
            .map_err(|e| OidcError::Http(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(OidcError::TokenExchangeFailed(status.as_u16(), body));
        }

        let kc_resp: KeycloakTokenResponse = resp
            .json()
            .await
            .map_err(|e| OidcError::Http(format!("failed to parse token response: {}", e)))?;

        Ok(TokenResponse {
            access_token: kc_resp.access_token,
            refresh_token: kc_resp.refresh_token,
            id_token: kc_resp.id_token,
            expires_in: kc_resp.expires_in,
            token_type: kc_resp.token_type,
        })
    }

    pub async fn refresh_access_token(&self, refresh_token: &str) -> Result<TokenResponse, OidcError> {
        let params = [
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("client_id", &self.client_id),
            ("client_secret", &self.client_secret),
        ];

        let resp = self
            .http
            .post(self.token_url.as_str())
            .form(&params)
            .send()
            .await
            .map_err(|e| OidcError::Http(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(OidcError::TokenExchangeFailed(status.as_u16(), body));
        }

        let kc_resp: KeycloakTokenResponse = resp
            .json()
            .await
            .map_err(|e| OidcError::Http(format!("failed to parse token response: {}", e)))?;

        Ok(TokenResponse {
            access_token: kc_resp.access_token,
            refresh_token: kc_resp.refresh_token,
            id_token: kc_resp.id_token,
            expires_in: kc_resp.expires_in,
            token_type: kc_resp.token_type,
        })
    }

    pub fn build_logout_url(&self, id_token_hint: Option<&str>, post_logout_redirect_uri: Option<&str>) -> String {
        let mut url = self.logout_url.clone();
        {
            let mut pairs = url.query_pairs_mut();
            if let Some(id_token) = id_token_hint {
                pairs.append_pair("id_token_hint", id_token);
            }
            if let Some(redirect) = post_logout_redirect_uri {
                pairs.append_pair("post_logout_redirect_uri", redirect);
            }
        }
        url.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> OidcConfig {
        OidcConfig {
            issuer: "http://localhost:8080/realms/bornemap".into(),
            client_id: "admin-dashboard".into(),
            client_secret: "test-secret".into(),
            redirect_uri: "http://localhost:5174/auth/callback".into(),
        }
    }

    #[test]
    fn test_build_authorize_url() {
        let config = test_config();
        let client = OidcClient::new(&config).unwrap();
        let url = client.build_authorize_url("test-state-123");
        assert!(url.contains("response_type=code"));
        assert!(url.contains("client_id=admin-dashboard"));
        assert!(url.contains("redirect_uri=http%3A%2F%2Flocalhost%3A5174%2Fauth%2Fcallback"));
        assert!(url.contains("state=test-state-123"));
        assert!(url.contains("scope=openid+email+profile"));
    }

    #[test]
    fn test_build_registration_url() {
        let config = test_config();
        let client = OidcClient::new(&config).unwrap();
        let url = client.build_registration_url("state-456");
        assert!(url.contains("kc_action=register"));
        assert!(url.contains("state=state-456"));
    }

    #[test]
    fn test_build_logout_url() {
        let config = test_config();
        let client = OidcClient::new(&config).unwrap();
        let url = client.build_logout_url(Some("id-token-abc"), Some("http://localhost:5174"));
        assert!(url.contains("id_token_hint=id-token-abc"));
        assert!(url.contains("post_logout_redirect_uri=http%3A%2F%2Flocalhost%3A5174"));
    }

    #[test]
    fn test_build_logout_url_no_hint() {
        let config = test_config();
        let client = OidcClient::new(&config).unwrap();
        let url = client.build_logout_url(None, None);
        assert!(!url.contains("id_token_hint="));
        assert!(!url.contains("post_logout_redirect_uri="));
    }

    #[test]
    fn test_oidc_error_display() {
        let err = OidcError::Config("missing field".into());
        assert_eq!(err.to_string(), "invalid configuration: missing field");

        let err = OidcError::Http("connection refused".into());
        assert_eq!(err.to_string(), "HTTP request failed: connection refused");
    }
}
