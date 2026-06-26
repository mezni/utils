use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: Uuid,
    pub email: String,
    pub email_verified: bool,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

impl User {
    /// Create a new user entity
    pub fn new(id: Uuid, email: String, email_verified: bool, status: String) -> Self {
        let now = Utc::now();
        User {
            id,
            email,
            email_verified,
            status,
            created_at: now,
            updated_at: now,
            deleted_at: None,
        }
    }

    /// Check if user is active
    pub fn is_active(&self) -> bool {
        self.status == "active" && self.deleted_at.is_none()
    }

    /// Check if user is verified
    pub fn is_verified(&self) -> bool {
        self.email_verified && self.is_active()
    }

    /// Soft delete user
    pub fn soft_delete(&mut self) {
        self.deleted_at = Some(Utc::now());
    }

    /// Update user information
    pub fn update(&mut self, email: Option<String>, email_verified: Option<bool>, status: Option<String>) {
        if let Some(email) = email {
            self.email = email;
        }
        if let Some(email_verified) = email_verified {
            self.email_verified = email_verified;
        }
        if let Some(status) = status {
            self.status = status;
        }
        self.updated_at = Utc::now();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshToken {
    pub id: Uuid,
    pub user_id: Uuid,
    pub jti: Uuid,
    pub token_hash: String,
    pub expires_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl RefreshToken {
    /// Create a new refresh token
    pub fn new(user_id: Uuid, jti: Uuid, expires_at: DateTime<Utc>) -> Self {
        let now = Utc::now();
        RefreshToken {
            id: Uuid::new_v4(),
            user_id,
            jti,
            token_hash: String::new(),
            expires_at,
            revoked_at: None,
            created_at: now,
        }
    }

    /// Check if token is expired
    pub fn is_expired(&self) -> bool {
        self.expires_at < Utc::now()
    }

    /// Check if token is revoked
    pub fn is_revoked(&self) -> bool {
        self.revoked_at.is_some()
    }

    /// Revoke token
    pub fn revoke(&mut self) {
        self.revoked_at = Some(Utc::now());
    }

    /// Set token hash for storage
    pub fn set_token_hash(&mut self, hash: String) {
        self.token_hash = hash;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_creation() {
        let user = User::new(
            Uuid::new_v4(),
            "test@example.com".to_string(),
            false,
            "active".to_string(),
        );

        assert_eq!(user.email, "test@example.com");
        assert!(!user.is_active());
        assert!(!user.is_verified());
    }

    #[test]
    fn test_user_is_active() {
        let mut user = User::new(
            Uuid::new_v4(),
            "test@example.com".to_string(),
            false,
            "active".to_string(),
        );

        assert!(user.is_active());

        user.soft_delete();

        assert!(!user.is_active());
    }

    #[test]
    fn test_user_update() {
        let mut user = User::new(
            Uuid::new_v4(),
            "test@example.com".to_string(),
            false,
            "active".to_string(),
        );

        user.update(Some("updated@example.com".to_string()), Some(true), Some("active".to_string()));

        assert_eq!(user.email, "updated@example.com");
        assert!(user.email_verified);
        assert_eq!(user.updated_at, Utc::now());
    }

    #[test]
    fn test_refresh_token_creation() {
        let user_id = Uuid::new_v4();
        let jti = Uuid::new_v4();
        let expires_at = Utc::now() + chrono::Duration::minutes(30);

        let token = RefreshToken::new(user_id, jti, expires_at);

        assert_eq!(token.user_id, user_id);
        assert_eq!(token.jti, jti);
        assert!(!token.is_expired());
        assert!(!token.is_revoked());
    }

    #[test]
    fn test_refresh_token_expiry() {
        let user_id = Uuid::new_v4();
        let jti = Uuid::new_v4();
        let expires_at = Utc::now() - chrono::Duration::minutes(30);

        let token = RefreshToken::new(user_id, jti, expires_at);

        assert!(token.is_expired());
    }

    #[test]
    fn test_refresh_token_revoke() {
        let mut token = RefreshToken::new(Uuid::new_v4(), Uuid::new_v4(), Utc::now() + chrono::Duration::minutes(30));

        assert!(!token.is_revoked());

        token.revoke();

        assert!(token.is_revoked());
    }
}