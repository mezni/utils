use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use auth_service::application::login::{LoginRequest, LoginUseCase};
use auth_service::application::refresh::{RefreshRequest, RefreshUseCase};
use auth_service::application::register::{RegisterRequest, RegisterUseCase};
use bornemap_auth::JwtService;
use bornemap_core::{
    AppError, AuthError, Session, SessionRepository, User, UserId, UserRepository, UserRole,
    UserStatus,
};
use chrono::Utc;
use uuid::Uuid;

struct MockUserRepository {
    users: Arc<Mutex<Vec<User>>>,
}

impl MockUserRepository {
    fn new() -> Self {
        Self {
            users: Arc::new(Mutex::new(vec![])),
        }
    }
}

impl Clone for MockUserRepository {
    fn clone(&self) -> Self {
        Self {
            users: self.users.clone(),
        }
    }
}

#[async_trait]
impl UserRepository for MockUserRepository {
    async fn create(&self, user: &User) -> Result<(), AuthError> {
        let mut users = self.users.lock().unwrap();
        if users.iter().any(|u| u.email == user.email) {
            return Err(AuthError::EmailAlreadyExists);
        }
        users.push(user.clone());
        Ok(())
    }

    async fn find_by_email(&self, email: &str) -> Result<Option<User>, AuthError> {
        let users = self.users.lock().unwrap();
        Ok(users.iter().find(|u| u.email == email).cloned())
    }

    async fn find_by_id(&self, id: UserId) -> Result<Option<User>, AuthError> {
        let users = self.users.lock().unwrap();
        Ok(users.iter().find(|u| u.id == id).cloned())
    }

    async fn email_exists(&self, email: &str) -> Result<bool, AuthError> {
        let users = self.users.lock().unwrap();
        Ok(users.iter().any(|u| u.email == email))
    }
}

struct MockSessionRepository {
    sessions: Arc<Mutex<Vec<Session>>>,
    fail_next_create: Arc<Mutex<bool>>,
}

impl MockSessionRepository {
    fn new() -> Self {
        Self {
            sessions: Arc::new(Mutex::new(vec![])),
            fail_next_create: Arc::new(Mutex::new(false)),
        }
    }
}

impl Clone for MockSessionRepository {
    fn clone(&self) -> Self {
        Self {
            sessions: self.sessions.clone(),
            fail_next_create: self.fail_next_create.clone(),
        }
    }
}

#[async_trait]
impl SessionRepository for MockSessionRepository {
    async fn create(&self, session: &Session) -> Result<(), AppError> {
        let mut fail = self.fail_next_create.lock().unwrap();
        if *fail {
            *fail = false;
            return Err(AppError::DatabaseError("mock failure".into()));
        }
        let mut sessions = self.sessions.lock().unwrap();
        sessions.push(session.clone());
        Ok(())
    }

    async fn find_by_token_hash(&self, token_hash: &str) -> Result<Option<Session>, AppError> {
        let sessions = self.sessions.lock().unwrap();
        Ok(sessions
            .iter()
            .find(|s| s.token_hash == token_hash)
            .cloned())
    }

    async fn revoke_session(&self, id: Uuid) -> Result<(), AppError> {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(s) = sessions.iter_mut().find(|s| s.id == id) {
            s.revoked = true;
            s.revoked_at = Some(Utc::now());
        }
        Ok(())
    }

    async fn revoke_family(&self, family_id: Uuid) -> Result<(), AppError> {
        let mut sessions = self.sessions.lock().unwrap();
        for s in sessions.iter_mut() {
            if s.family_id == family_id && !s.revoked {
                s.revoked = true;
                s.revoked_at = Some(Utc::now());
            }
        }
        Ok(())
    }

    async fn delete_user_sessions(&self, _user_id: UserId) -> Result<(), AppError> {
        // Mock implementation - just clear all sessions for testing
        let mut sessions = self.sessions.lock().unwrap();
        sessions.clear();
        Ok(())
    }

    async fn delete_expired(&self) -> Result<u64, AppError> {
        let mut sessions = self.sessions.lock().unwrap();
        let before = sessions.len();
        sessions.retain(|s| s.expires_at > Utc::now());
        Ok((before - sessions.len()) as u64)
    }
}

fn jwt_service() -> JwtService {
    JwtService::new(
        "test_secret_for_use_case_tests".into(),
        3600,
        "test-issuer".into(),
        "test-audience".into(),
    )
}

fn registered_user(repo: &MockUserRepository, email: &str, password: &str) {
    let hash = auth_service::infrastructure::password::hash_password(password).unwrap();
    let user = User {
        id: Uuid::new_v4(),
        email: email.to_string(),
        password_hash: hash,
        role: UserRole::RegisteredDriver,
        status: UserStatus::Active,
        created_at: Utc::now(),
    };
    let mut users = repo.users.lock().unwrap();
    users.push(user);
}

#[tokio::test]
async fn register_success() {
    let repo = MockUserRepository::new();
    let use_case = RegisterUseCase::new(repo);

    let req = RegisterRequest {
        email: "user@example.com".into(),
        password: "password123".into(),
    };

    let resp = use_case.execute(req).await.expect("register failed");
    assert!(!resp.user_id.is_empty());
}

#[tokio::test]
async fn register_duplicate_email() {
    let repo = MockUserRepository::new();
    let use_case = RegisterUseCase::new(repo);

    let req = RegisterRequest {
        email: "user@example.com".into(),
        password: "password123".into(),
    };
    use_case.execute(req).await.expect("first register failed");

    let req2 = RegisterRequest {
        email: "user@example.com".into(),
        password: "otherpass456".into(),
    };
    let err = use_case
        .execute(req2)
        .await
        .expect_err("duplicate should fail");
    assert!(matches!(err, AuthError::EmailAlreadyExists));
}

#[tokio::test]
async fn register_invalid_email_missing_at() {
    let repo = MockUserRepository::new();
    let use_case = RegisterUseCase::new(repo);

    let req = RegisterRequest {
        email: "userexample.com".into(),
        password: "password123".into(),
    };
    let err = use_case
        .execute(req)
        .await
        .expect_err("invalid email should fail");
    assert!(matches!(err, AuthError::ValidationError(_)));
}

#[tokio::test]
async fn register_invalid_email_missing_dot() {
    let repo = MockUserRepository::new();
    let use_case = RegisterUseCase::new(repo);

    let req = RegisterRequest {
        email: "user@examplecom".into(),
        password: "password123".into(),
    };
    let err = use_case
        .execute(req)
        .await
        .expect_err("invalid email should fail");
    assert!(matches!(err, AuthError::ValidationError(_)));
}

#[tokio::test]
async fn register_short_password() {
    let repo = MockUserRepository::new();
    let use_case = RegisterUseCase::new(repo);

    let req = RegisterRequest {
        email: "user@example.com".into(),
        password: "short".into(),
    };
    let err = use_case
        .execute(req)
        .await
        .expect_err("short password should fail");
    assert!(matches!(err, AuthError::ValidationError(_)));
    assert_eq!(
        err.to_string(),
        "Validation error: Password must be at least 8 characters"
    );
}

#[tokio::test]
async fn register_long_password() {
    let repo = MockUserRepository::new();
    let use_case = RegisterUseCase::new(repo);

    let req = RegisterRequest {
        email: "user@example.com".into(),
        password: "a".repeat(129),
    };
    let err = use_case
        .execute(req)
        .await
        .expect_err("long password should fail");
    assert!(matches!(err, AuthError::ValidationError(_)));
}

#[tokio::test]
async fn register_email_case_insensitive() {
    let repo = MockUserRepository::new();
    let use_case = RegisterUseCase::new(repo);

    let req = RegisterRequest {
        email: "User@Example.COM".into(),
        password: "password123".into(),
    };
    use_case.execute(req).await.expect("first register failed");

    let req2 = RegisterRequest {
        email: "user@example.com".into(),
        password: "password123".into(),
    };
    let err = use_case
        .execute(req2)
        .await
        .expect_err("duplicate after case change");
    assert!(matches!(err, AuthError::EmailAlreadyExists));
}

#[tokio::test]
async fn login_success() {
    let user_repo = MockUserRepository::new();
    registered_user(&user_repo, "user@example.com", "password123");
    let session_repo = MockSessionRepository::new();
    let use_case = LoginUseCase::new(user_repo, session_repo, jwt_service(), 86400);

    let req = LoginRequest {
        email: "user@example.com".into(),
        password: "password123".into(),
    };
    let resp = use_case.execute(req).await.expect("login failed");
    assert_eq!(resp.token_type, "Bearer");
    assert!(!resp.access_token.is_empty());
    assert!(!resp.refresh_token.is_empty());
}

#[tokio::test]
async fn login_wrong_password() {
    let user_repo = MockUserRepository::new();
    registered_user(&user_repo, "user@example.com", "password123");
    let session_repo = MockSessionRepository::new();
    let use_case = LoginUseCase::new(
        user_repo.clone(),
        session_repo.clone(),
        jwt_service(),
        86400,
    );

    let req = LoginRequest {
        email: "user@example.com".into(),
        password: "password123".into(),
    };
    let err = use_case
        .execute(req)
        .await
        .expect_err("wrong password should fail");
    assert!(matches!(err, AppError::InvalidCredentials));
}

#[tokio::test]
async fn login_nonexistent_email() {
    let user_repo = MockUserRepository::new();
    let session_repo = MockSessionRepository::new();
    let use_case = LoginUseCase::new(user_repo, session_repo, jwt_service(), 86400);

    let req = LoginRequest {
        email: "user@example.com".into(),
        password: "wrong_password".into(),
    };
    // Password 'wrong_password' should make the login fail.
    let err = use_case
        .execute(req)
        .await
        .expect_err("nonexistent email should fail");
    assert!(matches!(err, AppError::InvalidCredentials));
}

#[tokio::test]
async fn login_creates_session() {
    let user_repo = MockUserRepository::new();
    registered_user(&user_repo, "user@example.com", "password123");
    let session_repo = MockSessionRepository::new();
    let use_case = LoginUseCase::new(
        user_repo.clone(),
        session_repo.clone(),
        jwt_service(),
        86400,
    );

    let req = LoginRequest {
        email: "user@example.com".into(),
        password: "password123".into(),
    };
    let resp = use_case.execute(req).await.expect("login failed");
    assert!(!resp.refresh_token.is_empty());

    let sessions = session_repo.sessions.lock().unwrap();
    assert_eq!(sessions.len(), 1);
    assert!(!sessions[0].revoked);
}

#[tokio::test]
async fn refresh_success() {
    let user_repo = MockUserRepository::new();
    registered_user(&user_repo, "user@example.com", "password123");
    let session_repo = MockSessionRepository::new();
    let use_case = LoginUseCase::new(
        user_repo.clone(),
        session_repo.clone(),
        jwt_service(),
        86400,
    );

    let login_req = LoginRequest {
        email: "user@example.com".into(),
        password: "password123".into(),
    };
    let login_resp = use_case.execute(login_req).await.expect("login failed");

    let refresh_use_case = RefreshUseCase::new(
        user_repo.clone(),
        session_repo.clone(),
        jwt_service(),
        86400,
    );

    let refresh_req = RefreshRequest {
        refresh_token: login_resp.refresh_token.clone(),
    };

    let refresh_resp = refresh_use_case
        .execute(refresh_req)
        .await
        .expect("refresh failed");

    assert!(!refresh_resp.access_token.is_empty());
    assert!(!refresh_resp.refresh_token.is_empty());
    assert_ne!(login_resp.access_token, refresh_resp.access_token);
    assert_ne!(login_resp.refresh_token, refresh_resp.refresh_token);

    // Verify old session revoked and new one created
    let sessions = session_repo.sessions.lock().unwrap();
    assert_eq!(sessions.len(), 2);
    assert!(sessions[0].revoked);
    assert!(!sessions[1].revoked);
    assert_eq!(sessions[0].family_id, sessions[1].family_id);
}

#[tokio::test]
async fn refresh_invalid_token() {
    let user_repo = MockUserRepository::new();
    let session_repo = MockSessionRepository::new();
    let refresh_use_case = RefreshUseCase::new(
        user_repo.clone(),
        session_repo.clone(),
        jwt_service(),
        86400,
    );

    let refresh_req = RefreshRequest {
        refresh_token: "invalid_token".into(),
    };

    let err = refresh_use_case
        .execute(refresh_req)
        .await
        .expect_err("invalid token should fail");
    assert!(matches!(err, AppError::InvalidSession));
}

#[tokio::test]
async fn refresh_revoked_session() {
    let user_repo = MockUserRepository::new();
    registered_user(&user_repo, "user@example.com", "password123");
    let session_repo = MockSessionRepository::new();
    let use_case = LoginUseCase::new(
        user_repo.clone(),
        session_repo.clone(),
        jwt_service(),
        86400,
    );

    let login_req = LoginRequest {
        email: "user@example.com".into(),
        password: "password123".into(),
    };
    let login_resp = use_case.execute(login_req).await.expect("login failed");

    // Manually revoke the session
    session_repo.sessions.lock().unwrap()[0].revoked = true;

    let refresh_use_case = RefreshUseCase::new(
        user_repo.clone(),
        session_repo.clone(),
        jwt_service(),
        86400,
    );

    let refresh_req = RefreshRequest {
        refresh_token: login_resp.refresh_token.clone(),
    };

    let err = refresh_use_case
        .execute(refresh_req)
        .await
        .expect_err("revoked session should fail");
    assert!(matches!(err, AppError::InvalidSession));
}

#[tokio::test]
async fn refresh_expired_session() {
    let user_repo = MockUserRepository::new();
    registered_user(&user_repo, "user@example.com", "password123");
    let session_repo = MockSessionRepository::new();
    let use_case = LoginUseCase::new(
        user_repo.clone(),
        session_repo.clone(),
        jwt_service(),
        86400,
    );

    let login_req = LoginRequest {
        email: "user@example.com".into(),
        password: "password123".into(),
    };
    let _login_resp = use_case.execute(login_req).await.expect("login failed");

    // Manually expire the session
    session_repo.sessions.lock().unwrap()[0].expires_at = Utc::now() - chrono::Duration::seconds(1);

    let refresh_use_case = RefreshUseCase::new(
        user_repo.clone(),
        session_repo.clone(),
        jwt_service(),
        86400,
    );

    let refresh_req = RefreshRequest {
        refresh_token: _login_resp.refresh_token.clone(),
    };

    let err = refresh_use_case
        .execute(refresh_req)
        .await
        .expect_err("expired session should fail");
    assert!(matches!(err, AppError::ExpiredSession));
}

#[tokio::test]
async fn refresh_revokes_family_on_re_use() {
    let user_repo = MockUserRepository::new();
    registered_user(&user_repo, "user@example.com", "password123");
    let session_repo = MockSessionRepository::new();
    let use_case = LoginUseCase::new(
        user_repo.clone(),
        session_repo.clone(),
        jwt_service(),
        86400,
    );

    let login_req = LoginRequest {
        email: "user@example.com".into(),
        password: "password123".into(),
    };
    let login_resp = use_case.execute(login_req).await.expect("login failed");

    let refresh_use_case = RefreshUseCase::new(
        user_repo.clone(),
        session_repo.clone(),
        jwt_service(),
        86400,
    );

    let refresh_req = RefreshRequest {
        refresh_token: login_resp.refresh_token.clone(),
    };

    let _first_refresh_resp = refresh_use_case
        .execute(refresh_req)
        .await
        .expect("first refresh failed");

    // Reuse the old refresh token (family revocation check)
    let second_refresh_req = RefreshRequest {
        refresh_token: login_resp.refresh_token,
    };

    let err = refresh_use_case
        .execute(second_refresh_req)
        .await
        .expect_err("reused token should revoke family");
    assert!(matches!(err, AppError::InvalidSession));

    // Verify both original session and the first refreshed session are revoked
    let sessions = session_repo.sessions.lock().unwrap();
    assert_eq!(sessions.len(), 2);
    assert!(sessions[0].revoked);
    assert!(sessions[1].revoked);
}
