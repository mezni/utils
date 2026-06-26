#[cfg(test)]
mod tests {
    use super::*;
    use bornemap_auth::{OAuthProfile, OAuthStateStore};
    use bornemap_core::User;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    // Mock OAuth state store for testing
    struct MockOAuthStateStore {
        states: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    }

    impl MockOAuthStateStore {
        fn new() -> Self {
            Self {
                states: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
            }
        }
    }

    #[async_trait::async_trait]
    impl OAuthStateStore for MockOAuthStateStore {
        async fn create(&self, state: &str, _ttl: Duration) -> Result<(), AppError> {
            let mut states = self.states.lock().unwrap();
            states.insert(state.to_string());
            Ok(())
        }

        async fn consume(&self, state: &str) -> Result<bool, AppError> {
            let mut states = self.states.lock().unwrap();
            let existed = states.remove(state);
            Ok(existed)
        }
    }

    // Mock OAuth provider for testing
    struct MockOAuthProvider {
        should_fail: bool,
        profile: OAuthProfile,
    }

    impl MockOAuthProvider {
        fn new() -> Self {
            Self {
                should_fail: false,
                profile: OAuthProfile::new(
                    "mock-user-id".to_string(),
                    "test@example.com".to_string(),
                    true,
                    "mock".to_string(),
                )
                .with_name(Some("Test".to_string()), Some("User".to_string())),
            }
        }

        fn fail_next(mut self) -> Self {
            self.should_fail = true;
            self
        }
    }

    #[async_trait::async_trait]
    impl OAuthProvider for MockOAuthProvider {
        fn provider_name(&self) -> &'static str {
            "mock"
        }

        fn authorization_url(&self, state: &str, _redirect_uri: &str) -> String {
            format!("https://mock-provider/auth?state={}", state)
        }

        async fn exchange_code(&self, _code: String, _redirect_uri: &str) -> Result<OAuthTokenBundle, AppError> {
            if self.should_fail {
                return Err(AppError::OAuthTokenExchangeFailed("Mock token exchange failed".to_string()));
            }
            
            Ok(OAuthTokenBundle {
                access_token: "mock-access-token".to_string(),
                id_token: Some("mock-id-token".to_string()),
                refresh_token: Some("mock-refresh-token".to_string()),
            })
        }

        async fn fetch_profile(&self, _tokens: &OAuthTokenBundle) -> Result<OAuthProfile, AppError> {
            if self.should_fail {
                return Err(AppError::OAuthProfileFetchFailed("Mock profile fetch failed".to_string()));
            }
            
            Ok(self.profile.clone())
        }
    }

    // Mock OAuth repository for testing
    struct MockOAuthRepository {
        users: Arc<std::sync::Mutex<HashMap<String, User>>>,
        oauth_accounts: Arc<std::sync::Mutex<HashMap<String, OAuthAccount>>>,
    }

    impl MockOAuthRepository {
        fn new() -> Self {
            Self {
                users: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
                oauth_accounts: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            }
        }

        fn add_user(&self, user: User) {
            let mut users = self.users.lock().unwrap();
            users.insert(user.email.clone(), user);
        }

        fn add_oauth_account(&self, account: OAuthAccount) {
            let mut accounts = self.oauth_accounts.lock().unwrap();
            let key = format!("{}:{}", account.provider, account.provider_user_id);
            accounts.insert(key, account);
        }
    }

    impl MockOAuthRepository {
        async fn find_oauth_account(&self, provider: &str, provider_user_id: &str) -> Result<Option<OAuthAccount>, AppError> {
            let accounts = self.oauth_accounts.lock().unwrap();
            let key = format!("{}:{}", provider, provider_user_id);
            Ok(accounts.get(&key).cloned())
        }

        async fn find_oauth_account_by_email(&self, provider: &str, email: &str) -> Result<Option<OAuthAccount>, AppError> {
            let accounts = self.oauth_accounts.lock().unwrap();
            let account = accounts.values().find(|acc| acc.email == email && acc.provider == provider);
            Ok(account.cloned())
        }

        async fn create_oauth_account(&self, _user_id: uuid::Uuid, _profile: &OAuthProfile) -> Result<(), AppError> {
            Ok(())
        }

        async fn find_user_by_email(&self, email: &str) -> Result<Option<User>, AppError> {
            let users = self.users.lock().unwrap();
            Ok(users.get(email).cloned())
        }

        async fn link_oauth_account(&self, _user_id: uuid::Uuid, _profile: &OAuthProfile) -> Result<(), AppError> {
            Ok(())
        }

        async fn create_user_with_oauth(&self, email: &str, _profile: &OAuthProfile) -> Result<User, AppError> {
            let user = User {
                id: uuid::Uuid::new_v4(),
                email: email.to_string(),
                password_hash: String::new(),
                role: bornemap_core::UserRole::RegisteredDriver,
                status: bornemap_core::UserStatus::Active,
                created_at: chrono::Utc::now(),
            };
            self.add_user(user.clone());
            Ok(user)
        }
    }

    #[tokio::test]
    async fn test_oauth_start_use_case() {
        let state_store = MockOAuthStateStore::new();
        let provider = MockOAuthProvider::new();
        let use_case = OAuthStartUseCase::new(provider, state_store);

        let redirect_uri = "http://localhost:8080/callback";
        let result = use_case.execute(redirect_uri).await;

        assert!(result.is_ok());
        let auth_url = result.unwrap();
        assert!(auth_url.contains("https://mock-provider/auth"));
        assert!(auth_url.contains("state="));
    }

    #[tokio::test]
    async fn test_oauth_callback_use_case_new_user() {
        let state_store = MockOAuthStateStore::new();
        let provider = MockOAuthProvider::new();
        let repository = MockOAuthRepository::new();
        let use_case = OAuthCallbackUseCase::new(provider, state_store, repository);

        let code = "mock-auth-code".to_string();
        let state = uuid::Uuid::new_v4().to_string();
        
        // Create state first
        state_store.create(&state, Duration::from_secs(300)).await.unwrap();
        
        let redirect_uri = "http://localhost:8080/callback";
        let result = use_case.execute(code, state, redirect_uri).await;

        assert!(result.is_ok());
        let user = result.unwrap();
        assert_eq!(user.email, "test@example.com");
        assert_eq!(user.role, bornemap_core::UserRole::RegisteredDriver);
    }

    #[tokio::test]
    async fn test_oauth_callback_use_case_existing_user() {
        let state_store = MockOAuthStateStore::new();
        let provider = MockOAuthProvider::new();
        let repository = MockOAuthRepository::new();
        
        // Add existing user
        let existing_user = User {
            id: uuid::Uuid::new_v4(),
            email: "existing@example.com".to_string(),
            password_hash: "hashed-password".to_string(),
            role: bornemap_core::UserRole::Partner,
            status: bornemap_core::UserStatus::Active,
            created_at: chrono::Utc::now(),
        };
        repository.add_user(existing_user.clone());

        let use_case = OAuthCallbackUseCase::new(provider, state_store, repository);

        let code = "mock-auth-code".to_string();
        let state = uuid::Uuid::new_v4().to_string();
        
        // Create state first
        state_store.create(&state, Duration::from_secs(300)).await.unwrap();
        
        let redirect_uri = "http://localhost:8080/callback";
        let result = use_case.execute(code, state, redirect_uri).await;

        assert!(result.is_ok());
        let user = result.unwrap();
        assert_eq!(user.email, "existing@example.com");
        assert_eq!(user.role, bornemap_core::UserRole::Partner);
    }

    #[tokio::test]
    async fn test_oauth_callback_use_case_invalid_state() {
        let state_store = MockOAuthStateStore::new();
        let provider = MockOAuthProvider::new();
        let repository = MockOAuthRepository::new();
        let use_case = OAuthCallbackUseCase::new(provider, state_store, repository);

        let code = "mock-auth-code".to_string();
        let state = "invalid-state".to_string();
        
        let redirect_uri = "http://localhost:8080/callback";
        let result = use_case.execute(code, state, redirect_uri).await;

        assert!(result.is_err());
        assert!(matches!(result, Err(AppError::OAuthStateInvalid)));
    }

    #[tokio::test]
    async fn test_oauth_callback_use_case_token_exchange_failure() {
        let state_store = MockOAuthStateStore::new();
        let provider = MockOAuthProvider::new().fail_next();
        let repository = MockOAuthRepository::new();
        let use_case = OAuthCallbackUseCase::new(provider, state_store, repository);

        let code = "mock-auth-code".to_string();
        let state = uuid::Uuid::new_v4().to_string();
        
        // Create state first
        state_store.create(&state, Duration::from_secs(300)).await.unwrap();
        
        let redirect_uri = "http://localhost:8080/callback";
        let result = use_case.execute(code, state, redirect_uri).await;

        assert!(result.is_err());
        assert!(matches!(result, Err(AppError::OAuthTokenExchangeFailed(_))));
    }

    #[tokio::test]
    async fn test_oauth_callback_use_case_profile_fetch_failure() {
        let mut provider = MockOAuthProvider::new();
        provider.should_fail = true;
        
        let state_store = MockOAuthStateStore::new();
        let repository = MockOAuthRepository::new();
        let use_case = OAuthCallbackUseCase::new(provider, state_store, repository);

        let code = "mock-auth-code".to_string();
        let state = uuid::Uuid::new_v4().to_string();
        
        // Create state first
        state_store.create(&state, Duration::from_secs(300)).await.unwrap();
        
        let redirect_uri = "http://localhost:8080/callback";
        let result = use_case.execute(code, state, redirect_uri).await;

        assert!(result.is_err());
        assert!(matches!(result, Err(AppError::OAuthProfileFetchFailed(_))));
    }
}