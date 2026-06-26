use std::sync::Mutex;

use auth_service::application::login::{LoginRequest, LoginUseCase};
use auth_service::application::register::{RegisterRequest, RegisterUseCase};
use async_trait::async_trait;
use bornemap_auth::JwtService;
use bornemap_core::{AuthError, User, UserId, UserRepository, UserRole, UserStatus};
use chrono::Utc;
use uuid::Uuid;

struct MockUserRepository {
    users: Mutex<Vec<User>>,
}

impl MockUserRepository {
    fn new() -> Self {
        Self {
            users: Mutex::new(vec![]),
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

fn jwt_service() -> JwtService {
    JwtService::new("test_secret_for_use_case_tests".into(), 3600)
}

fn registered_user(repo: &MockUserRepository, email: &str, password: &str) {
    let hash = auth_service::infrastructure::password::PasswordService::hash(password).unwrap();
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
    let use_case = RegisterUseCase::new(repo, jwt_service());

    let req = RegisterRequest {
        email: "user@example.com".into(),
        password: "password123".into(),
    };

    let resp = use_case.execute(req).await.expect("register failed");
    assert_eq!(resp.token_type, "Bearer");
    assert_eq!(resp.expires_in, 86400);
    assert!(!resp.access_token.is_empty());
}

#[tokio::test]
async fn register_duplicate_email() {
    let repo = MockUserRepository::new();
    let use_case = RegisterUseCase::new(repo, jwt_service());

    let req = RegisterRequest {
        email: "user@example.com".into(),
        password: "password123".into(),
    };
    use_case.execute(req).await.expect("first register failed");

    let req2 = RegisterRequest {
        email: "user@example.com".into(),
        password: "otherpass456".into(),
    };
    let err = use_case.execute(req2).await.expect_err("duplicate should fail");
    assert!(matches!(err, AuthError::EmailAlreadyExists));
}

#[tokio::test]
async fn register_invalid_email_missing_at() {
    let repo = MockUserRepository::new();
    let use_case = RegisterUseCase::new(repo, jwt_service());

    let req = RegisterRequest {
        email: "userexample.com".into(),
        password: "password123".into(),
    };
    let err = use_case.execute(req).await.expect_err("invalid email should fail");
    assert!(matches!(err, AuthError::ValidationError(_)));
}

#[tokio::test]
async fn register_invalid_email_missing_dot() {
    let repo = MockUserRepository::new();
    let use_case = RegisterUseCase::new(repo, jwt_service());

    let req = RegisterRequest {
        email: "user@examplecom".into(),
        password: "password123".into(),
    };
    let err = use_case.execute(req).await.expect_err("invalid email should fail");
    assert!(matches!(err, AuthError::ValidationError(_)));
}

#[tokio::test]
async fn register_short_password() {
    let repo = MockUserRepository::new();
    let use_case = RegisterUseCase::new(repo, jwt_service());

    let req = RegisterRequest {
        email: "user@example.com".into(),
        password: "short".into(),
    };
    let err = use_case.execute(req).await.expect_err("short password should fail");
    assert!(matches!(err, AuthError::ValidationError(_)));
    assert_eq!(err.to_string(), "Validation error: Password must be at least 8 characters");
}

#[tokio::test]
async fn register_long_password() {
    let repo = MockUserRepository::new();
    let use_case = RegisterUseCase::new(repo, jwt_service());

    let req = RegisterRequest {
        email: "user@example.com".into(),
        password: "a".repeat(129),
    };
    let err = use_case.execute(req).await.expect_err("long password should fail");
    assert!(matches!(err, AuthError::ValidationError(_)));
}

#[tokio::test]
async fn register_email_case_insensitive() {
    let repo = MockUserRepository::new();
    let use_case = RegisterUseCase::new(repo, jwt_service());

    let req = RegisterRequest {
        email: "User@Example.COM".into(),
        password: "password123".into(),
    };
    use_case.execute(req).await.expect("first register failed");

    let req2 = RegisterRequest {
        email: "user@example.com".into(),
        password: "password123".into(),
    };
    let err = use_case.execute(req2).await.expect_err("duplicate after case change");
    assert!(matches!(err, AuthError::EmailAlreadyExists));
}

#[tokio::test]
async fn login_success() {
    let repo = MockUserRepository::new();
    registered_user(&repo, "user@example.com", "password123");
    let use_case = LoginUseCase::new(repo, jwt_service());

    let req = LoginRequest {
        email: "user@example.com".into(),
        password: "password123".into(),
    };
    let resp = use_case.execute(req).await.expect("login failed");
    assert_eq!(resp.token_type, "Bearer");
    assert!(!resp.access_token.is_empty());
}

#[tokio::test]
async fn login_wrong_password() {
    let repo = MockUserRepository::new();
    registered_user(&repo, "user@example.com", "password123");
    let use_case = LoginUseCase::new(repo, jwt_service());

    let req = LoginRequest {
        email: "user@example.com".into(),
        password: "wrongpassword".into(),
    };
    let err = use_case.execute(req).await.expect_err("wrong password should fail");
    assert!(matches!(err, AuthError::InvalidCredentials));
}

#[tokio::test]
async fn login_nonexistent_email() {
    let repo = MockUserRepository::new();
    let use_case = LoginUseCase::new(repo, jwt_service());

    let req = LoginRequest {
        email: "nobody@example.com".into(),
        password: "password123".into(),
    };
    let err = use_case.execute(req).await.expect_err("nonexistent email should fail");
    assert!(matches!(err, AuthError::InvalidCredentials));
}
