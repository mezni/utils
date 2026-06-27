use async_trait::async_trait;
use bornemap_core::AppError;
use std::time::Duration;

#[async_trait]
pub trait OAuthStateStore: Send + Sync {
    async fn create(&self, state: &str, ttl: Duration) -> Result<(), AppError>;

    async fn consume(&self, state: &str) -> Result<bool, AppError>;
}

#[cfg(test)]
pub mod mock {
    use super::*;
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};

    pub struct MockOAuthStateStore {
        states: Arc<Mutex<HashSet<String>>>,
    }

    impl MockOAuthStateStore {
        pub fn new() -> Self {
            Self {
                states: Arc::new(Mutex::new(HashSet::new())),
            }
        }
    }

    #[async_trait]
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_state_creation_and_consumption() {
        let store = mock::MockOAuthStateStore::new();
        let state = "test-state-123";

        store.create(state, Duration::from_secs(300)).await.unwrap();
        let result = store.consume(state).await.unwrap();
        assert!(result);

        let result = store.consume(state).await.unwrap();
        assert!(!result);
    }

    #[tokio::test]
    async fn test_consume_nonexistent_state() {
        let store = mock::MockOAuthStateStore::new();
        let result = store.consume("nonexistent-state").await.unwrap();
        assert!(!result);
    }
}
