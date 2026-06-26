use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthTokenBundle {
    pub access_token: String,
    pub id_token: Option<String>,
    pub refresh_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthProfile {
    pub provider_user_id: String,
    pub email: String,
    pub email_verified: bool,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub avatar_url: Option<String>,
    pub provider: String,
    pub raw_attributes: Option<HashMap<String, serde_json::Value>>,
}

impl OAuthProfile {
    pub fn new(
        provider_user_id: String,
        email: String,
        email_verified: bool,
        provider: String,
    ) -> Self {
        Self {
            provider_user_id,
            email,
            email_verified,
            first_name: None,
            last_name: None,
            avatar_url: None,
            provider,
            raw_attributes: None,
        }
    }

    pub fn with_name(mut self, first_name: Option<String>, last_name: Option<String>) -> Self {
        self.first_name = first_name;
        self.last_name = last_name;
        self
    }

    pub fn with_avatar(mut self, avatar_url: Option<String>) -> Self {
        self.avatar_url = avatar_url;
        self
    }

    pub fn with_raw_attributes(mut self, attributes: HashMap<String, serde_json::Value>) -> Self {
        self.raw_attributes = Some(attributes);
        self
    }

    pub fn full_name(&self) -> Option<String> {
        match (self.first_name.as_ref(), self.last_name.as_ref()) {
            (Some(first), Some(last)) => Some(format!("{} {}", first, last)),
            (Some(first), None) => Some(first.clone()),
            (None, Some(last)) => Some(last.clone()),
            (None, None) => None,
        }
    }
}