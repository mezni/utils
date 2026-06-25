use std::collections::HashMap;


use chrono::{DateTime, Duration, Utc};
use tokio::sync::RwLock;
use uuid::Uuid;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionData {
    pub session_id: Uuid,
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub user_uuid: Uuid,
    pub roles: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

pub struct SessionManager {
    sessions: RwLock<HashMap<Uuid, SessionData>>,
    states: RwLock<HashMap<String, DateTime<Utc>>>,
    session_ttl: Duration,
    state_ttl: Duration,
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            states: RwLock::new(HashMap::new()),
            session_ttl: Duration::hours(24),
            state_ttl: Duration::minutes(10),
        }
    }

    pub async fn create_state(&self) -> String {
        let state = Uuid::new_v4().to_string();
        let mut states = self.states.write().await;
        self.cleanup_expired_states(&mut states);
        states.insert(state.clone(), Utc::now());
        state
    }

    pub async fn verify_and_consume_state(&self, state: &str) -> bool {
        let mut states = self.states.write().await;
        states.remove(state).is_some()
    }

    pub async fn create_session(
        &self,
        access_token: String,
        refresh_token: Option<String>,
        user_uuid: Uuid,
        roles: Vec<String>,
    ) -> SessionData {
        let now = Utc::now();
        let session = SessionData {
            session_id: Uuid::new_v4(),
            access_token,
            refresh_token,
            user_uuid,
            roles,
            created_at: now,
            expires_at: now + self.session_ttl,
        };

        let mut sessions = self.sessions.write().await;
        self.cleanup_expired_sessions(&mut sessions);
        sessions.insert(session.session_id, session.clone());
        session
    }

    pub async fn get_session(&self, session_id: Uuid) -> Option<SessionData> {
        let sessions = self.sessions.read().await;
        sessions.get(&session_id).filter(|s| s.expires_at > Utc::now()).cloned()
    }

    pub async fn delete_session(&self, session_id: Uuid) {
        let mut sessions = self.sessions.write().await;
        sessions.remove(&session_id);
    }

    fn cleanup_expired_sessions(&self, sessions: &mut HashMap<Uuid, SessionData>) {
        let now = Utc::now();
        sessions.retain(|_, s| s.expires_at > now);
    }

    fn cleanup_expired_states(&self, states: &mut HashMap<String, DateTime<Utc>>) {
        let now = Utc::now();
        states.retain(|_, created| now.signed_duration_since(*created) < self.state_ttl);
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_session() {
        let manager = SessionManager::new();
        let uid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let session = manager
            .create_session("tok".into(), Some("ref".into()), uid, vec!["admin".into()])
            .await;
        assert_eq!(session.user_uuid, uid);
        assert_eq!(session.access_token, "tok");
        assert_eq!(session.roles, vec!["admin"]);
    }

    #[tokio::test]
    async fn test_get_session_after_delete() {
        let manager = SessionManager::new();
        let uid = Uuid::nil();
        let s = manager.create_session("t".into(), None, uid, vec![]).await;
        manager.delete_session(s.session_id).await;
        assert!(manager.get_session(s.session_id).await.is_none());
    }

    #[tokio::test]
    async fn test_state_flow() {
        let manager = SessionManager::new();
        let state = manager.create_state().await;
        assert!(manager.verify_and_consume_state(&state).await);
        assert!(!manager.verify_and_consume_state(&state).await);
    }
}
