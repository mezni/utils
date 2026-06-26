#[cfg(test)]
mod tests {
    use super::*;
    use bornemap_auth::OAuthProfile;
    use std::time::Duration;

    struct MockOAuthStateStore {
        states: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    }

    impl MockOAuthStateStore {
        fn new() -> Self {
            Self {
                states: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
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

    #[test]
    fn test_oauth_profile_creation() {
        let profile = OAuthProfile::new(
            "google-123".to_string(),
            "test@example.com".to_string(),
            true,
            "google".to_string(),
        );

        assert_eq!(profile.provider_user_id, "google-123");
        assert_eq!(profile.email, "test@example.com");
        assert_eq!(profile.email_verified, true);
        assert_eq!(profile.provider, "google");
        assert!(profile.first_name.is_none());
        assert!(profile.last_name.is_none());
        assert!(profile.avatar_url.is_none());
    }

    #[test]
    fn test_oauth_profile_with_name() {
        let profile = OAuthProfile::new(
            "google-123".to_string(),
            "test@example.com".to_string(),
            true,
            "google".to_string(),
        )
        .with_name(Some("John".to_string()), Some("Doe".to_string()));

        assert_eq!(profile.first_name, Some("John".to_string()));
        assert_eq!(profile.last_name, Some("Doe".to_string()));
        assert_eq!(profile.full_name(), Some("John Doe".to_string()));
    }

    #[test]
    fn test_oauth_profile_single_name() {
        let profile = OAuthProfile::new(
            "google-123".to_string(),
            "test@example.com".to_string(),
            true,
            "google".to_string(),
        )
        .with_name(Some("John".to_string()), None);

        assert_eq!(profile.full_name(), Some("John".to_string()));
    }

    #[test]
    fn test_oauth_profile_no_name() {
        let profile = OAuthProfile::new(
            "google-123".to_string(),
            "test@example.com".to_string(),
            true,
            "google".to_string(),
        )
        .with_name(None, None);

        assert_eq!(profile.full_name(), None);
    }

    #[test]
    fn test_oauth_profile_with_avatar() {
        let profile = OAuthProfile::new(
            "google-123".to_string(),
            "test@example.com".to_string(),
            true,
            "google".to_string(),
        )
        .with_avatar(Some("https://example.com/avatar.jpg".to_string()));

        assert_eq!(profile.avatar_url, Some("https://example.com/avatar.jpg".to_string()));
    }

    #[tokio::test]
    async fn test_oauth_state_store() {
        let store = MockOAuthStateStore::new();
        let state = "test-state-123";

        // Test state creation
        store.create(state, Duration::from_secs(300)).await.unwrap();
        
        // Test state consumption (should succeed)
        let result = store.consume(state).await.unwrap();
        assert!(result);

        // Test consume again (should fail)
        let result = store.consume(state).await.unwrap();
        assert!(!result);
    }

    #[tokio::test]
    async fn test_oauth_state_store_nonexistent() {
        let store = MockOAuthStateStore::new();
        let result = store.consume("nonexistent-state").await.unwrap();
        assert!(!result);
    }

    #[test]
    fn test_google_oauth_provider_authorization_url() {
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
    fn test_google_oauth_provider_scopes() {
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