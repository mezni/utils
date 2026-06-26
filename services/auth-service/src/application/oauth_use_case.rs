use async_trait::async_trait;
use bornemap_core::{AppError, User, UserId};
use bornemap_auth::{OAuthProfile, OAuthProvider, OAuthStateStore, OAuthTokenBundle};
use bornemap_auth::OAuthProfile as OAuthProfileStruct;
use crate::infrastructure::oauth_repository::PgOAuthRepository;
use std::time::Duration;

pub struct OAuthStartUseCase<P: OAuthProvider, S: OAuthStateStore> {
    provider: P,
    state_store: S,
}

impl<P: OAuthProvider, S: OAuthStateStore> OAuthStartUseCase<P, S> {
    pub fn new(provider: P, state_store: S) -> Self {
        Self { provider, state_store }
    }

    pub async fn execute(&self, redirect_uri: &str) -> Result<String, AppError> {
        // Generate a random state
        let state = uuid::Uuid::new_v4().to_string();
        
        // Store the state with TTL (5 minutes)
        let ttl = Duration::from_secs(300);
        self.state_store.create(&state, ttl).await?;
        
        // Generate authorization URL
        Ok(self.provider.authorization_url(&state, redirect_uri))
    }
}

pub struct OAuthCallbackUseCase<P: OAuthProvider, S: OAuthStateStore, R: PgOAuthRepository> {
    provider: P,
    state_store: S,
    oauth_repository: R,
}

impl<P: OAuthProvider, S: OAuthStateStore, R: PgOAuthRepository> OAuthCallbackUseCase<P, S, R> {
    pub fn new(provider: P, state_store: S, oauth_repository: R) -> Self {
        Self { provider, state_store, oauth_repository }
    }

    pub async fn execute(&self, code: String, state: String, redirect_uri: &str) -> Result<User, AppError> {
        // Validate and consume the state
        let state_valid = self.state_store.consume(&state).await?;
        if !state_valid {
            return Err(AppError::OAuthStateInvalid);
        }

        // Exchange authorization code for tokens
        let tokens = self.provider.exchange_code(code, redirect_uri).await?;
        
        // Fetch user profile
        let profile = self.provider.fetch_profile(&tokens).await?;

        // Handle OAuth authentication flow
        self.handle_oauth_flow(&profile).await
    }

    async fn handle_oauth_flow(&self, profile: &OAuthProfile) -> Result<User, AppError> {
        // Check if OAuth account exists
        if let Some(oauth_account) = self.oauth_repository.find_oauth_account(&profile.provider, &profile.provider_user_id).await? {
            // User exists, return the user
            return self.oauth_repository.find_user_by_email(&profile.email).await?
                .ok_or_else(|| AppError::UserNotFound);
        }

        // Check if user with this email already exists
        if let Some(existing_user) = self.oauth_repository.find_user_by_email(&profile.email).await? {
            // Link OAuth account to existing user
            self.oauth_repository.link_oauth_account(existing_user.id, profile).await?;
            return Ok(existing_user);
        }

        // Create new user with OAuth account
        self.oauth_repository.create_user_with_oauth(&profile.email, profile).await
    }
}