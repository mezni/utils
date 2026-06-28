use chrono::Utc;
use uuid::Uuid;

use common_auth::roles::Role;
use common_errors::AppError;

use crate::domain::account::Account;
use crate::domain::error::DomainError;
use crate::domain::repository::AccountRepository;
use crate::infrastructure::jwt_service::JwtService;
use crate::infrastructure::password::PasswordService;

pub struct RegisterRequest {
    pub email: String,
    pub password: String,
    pub role: String,
}

#[derive(Debug)]
pub struct RegisterResponse {
    pub token: String,
    pub email: String,
    pub role: String,
}

pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug)]
pub struct LoginResponse {
    pub token: String,
    pub email: String,
    pub role: String,
}

pub struct AuthUseCases<R: AccountRepository> {
    repo: R,
    jwt: JwtService,
}

impl<R: AccountRepository> AuthUseCases<R> {
    pub fn new(repo: R, jwt: JwtService) -> Self {
        Self { repo, jwt }
    }

    pub async fn register(&self, req: RegisterRequest) -> Result<RegisterResponse, AppError> {
        if req.email.trim().is_empty() || !req.email.contains('@') {
            return Err(AppError::BadRequest("Invalid email format".into()));
        }

        let role = Role::try_from(req.role.as_str())
            .map_err(|_| AppError::BadRequest(format!("Invalid role: {}", req.role)))?;

        let password_hash =
            PasswordService::hash(&req.password).map_err(|e| AppError::BadRequest(e.to_string()))?;

        let account = Account {
            id: Uuid::new_v4(),
            email: req.email.to_lowercase().trim().to_string(),
            password_hash,
            role: req.role.to_lowercase(),
            created_at: Utc::now(),
        };

        self.repo.create(&account).await.map_err(|e| match e {
            DomainError::AlreadyExists => {
                AppError::Conflict("An account with this email already exists".into())
            }
            _ => AppError::Internal("Failed to create account".into()),
        })?;

        let token = self
            .jwt
            .generate_token(account.id, role)
            .map_err(|e| AppError::Internal(format!("Token generation failed: {e}")))?;

        Ok(RegisterResponse {
            token,
            email: account.email,
            role: account.role,
        })
    }

    pub async fn login(&self, req: LoginRequest) -> Result<LoginResponse, AppError> {
        let account = self
            .repo
            .find_by_email(req.email.to_lowercase().trim())
            .await
            .map_err(|_| AppError::Internal("Database error".into()))?
            .ok_or_else(|| AppError::Unauthorized("Invalid email or password".into()))?;

        let valid = PasswordService::verify(&req.password, &account.password_hash);
        if !valid {
            return Err(AppError::Unauthorized("Invalid email or password".into()));
        }

        let role = Role::try_from(account.role.as_str())
            .map_err(|_| AppError::Internal("Invalid role stored in database".into()))?;

        let token = self
            .jwt
            .generate_token(account.id, role)
            .map_err(|e| AppError::Internal(format!("Token generation failed: {e}")))?;

        Ok(LoginResponse {
            token,
            email: account.email,
            role: account.role,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use uuid::Uuid;

    use crate::domain::account::Account;

    struct MockRepo {
        accounts: Mutex<HashMap<String, Account>>,
    }

    impl MockRepo {
        fn new() -> Self {
            Self {
                accounts: Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl AccountRepository for MockRepo {
        async fn create(&self, account: &Account) -> Result<(), DomainError> {
            let mut map = self.accounts.lock().unwrap();
            if map.contains_key(&account.email) {
                return Err(DomainError::AlreadyExists);
            }
            map.insert(account.email.clone(), account.clone());
            Ok(())
        }

        async fn find_by_email(&self, email: &str) -> Result<Option<Account>, DomainError> {
            let map = self.accounts.lock().unwrap();
            Ok(map.get(email).cloned())
        }

        async fn find_by_id(&self, _id: Uuid) -> Result<Option<Account>, DomainError> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn test_register_success() {
        let repo = MockRepo::new();
        let jwt = JwtService::new("test-secret".to_string());
        let uc = AuthUseCases::new(repo, jwt);

        let result = uc
            .register(RegisterRequest {
                email: "test@example.com".into(),
                password: "password123".into(),
                role: "driver".into(),
            })
            .await;

        assert!(result.is_ok());
        let resp = result.unwrap();
        assert_eq!(resp.email, "test@example.com");
        assert_eq!(resp.role, "driver");
    }

    #[tokio::test]
    async fn test_register_duplicate_email() {
        let repo = MockRepo::new();
        let jwt = JwtService::new("test-secret".to_string());
        let uc = AuthUseCases::new(repo, jwt);

        uc.register(RegisterRequest {
            email: "dup@example.com".into(),
            password: "password123".into(),
            role: "driver".into(),
        })
        .await
        .unwrap();

        let result = uc
            .register(RegisterRequest {
                email: "dup@example.com".into(),
                password: "password123".into(),
                role: "driver".into(),
            })
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::Conflict(_) => {}
            _ => panic!("Expected Conflict error"),
        }
    }

    #[tokio::test]
    async fn test_register_invalid_email() {
        let repo = MockRepo::new();
        let jwt = JwtService::new("test-secret".to_string());
        let uc = AuthUseCases::new(repo, jwt);

        let result = uc
            .register(RegisterRequest {
                email: "not-an-email".into(),
                password: "password123".into(),
                role: "driver".into(),
            })
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_register_weak_password() {
        let repo = MockRepo::new();
        let jwt = JwtService::new("test-secret".to_string());
        let uc = AuthUseCases::new(repo, jwt);

        let result = uc
            .register(RegisterRequest {
                email: "test@example.com".into(),
                password: "short".into(),
                role: "driver".into(),
            })
            .await;

        assert!(result.is_err());
    }
}
