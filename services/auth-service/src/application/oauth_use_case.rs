use bornemap_core::AppError;
use crate::application::oauth_state::OAuthStateStore;
use std::time::Duration;

pub struct OAuthStartUseCase<S: OAuthStateStore> {
    state_store: S,
}

impl<S: OAuthStateStore> OAuthStartUseCase<S> {
    pub fn new(state_store: S) -> Self {
        Self { state_store }
    }

    pub async fn execute(&self, _redirect_uri: &str) -> Result<String, AppError> {
        let state = uuid::Uuid::new_v4().to_string();
        let ttl = Duration::from_secs(300);
        self.state_store.create(&state, ttl).await?;
        Ok(state)
    }
}

pub struct OAuthCallbackUseCase<S: OAuthStateStore> {
    state_store: S,
}

impl<S: OAuthStateStore> OAuthCallbackUseCase<S> {
    pub fn new(state_store: S) -> Self {
        Self { state_store }
    }

    pub async fn execute(&self, state: &str) -> Result<bool, AppError> {
        self.state_store.validate_oauth_state(state).await
    }
}