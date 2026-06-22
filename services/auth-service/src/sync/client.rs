use reqwest::Client;
use uuid::Uuid;

use domain_types::user::UserProfile;

#[derive(Debug, thiserror::Error)]
pub enum SyncClientError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Service returned error: {0}")]
    Service(String),
}

#[derive(Clone)]
pub struct AuthSyncClient {
    client: Client,
    base_url: String,
    auth_token: String,
}

impl AuthSyncClient {
    pub fn new(base_url: impl Into<String>, auth_token: impl Into<String>) -> Self {
        Self {
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("Failed to create HTTP client"),
            base_url: base_url.into(),
            auth_token: auth_token.into(),
        }
    }

    pub async fn sync_user(&self, user_uuid: Uuid, email: Option<&str>, role: Option<&str>) -> Result<UserProfile, SyncClientError> {
        let mut url = format!("{}/api/v1/auth/sync?user_uuid={}", self.base_url, user_uuid);
        if let Some(e) = email {
            url.push_str(&format!("&email={}", e));
        }
        if let Some(r) = role {
            url.push_str(&format!("&role={}", r));
        }

        let resp = self.client
            .get(&url)
            .bearer_auth(&self.auth_token)
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(SyncClientError::Service(format!(
                "Sync request failed with status: {}",
                resp.status()
            )));
        }

        #[derive(serde::Deserialize)]
        struct SyncResp {
            status: String,
            profile: UserProfile,
        }

        let sync_resp: SyncResp = resp.json().await?;
        Ok(sync_resp.profile)
    }
}
