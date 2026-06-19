use actix_web::{dev::RequestHead, http::header, Error, HttpRequest};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserContext {
    pub user_id: String,
    pub roles: Vec<String>,
}

pub fn extract_user_context(req: &HttpRequest) -> Result<UserContext, AuthError> {
    // Extract X-User-Id from Traefik headers
    let user_id = req
        .headers()
        .get(header::HeaderName::from_static("X-User-Id"))
        .and_then(|header| header.to_str().ok())
        .ok_or_else(|| AuthError::Unauthorized)?;

    if user_id.is_empty() {
        return Err(AuthError::Unauthorized);
    }

    // Extract X-User-Roles from Traefik headers
    let roles_str = req
        .headers()
        .get(header::HeaderName::from_static("X-User-Roles"))
        .and_then(|header| header.to_str().ok())
        .unwrap_or("role:admin"); // Default to admin if not specified

    let roles: Vec<String> = roles_str
        .split(',')
        .map(|role| role.trim().to_string())
        .filter(|role| !role.is_empty())
        .collect();

    Ok(UserContext {
        user_id: user_id.to_string(),
        roles,
    })
}

pub fn validate_roles(user_context: &UserContext, required_roles: &[&str]) -> Result<(), AuthError> {
    let user_has_role = user_context.roles.iter().any(|role| {
        required_roles.contains(&role.as_str()) || role == "*"
    });

    if !user_has_role {
        return Err(AuthError::Forbidden(format!(
            "User {} does not have required roles. Required roles: {}",
            user_context.user_id,
            required_roles.join(", ")
        )));
    }

    Ok(())
}

#[derive(Clone)]
pub struct AuthMiddleware {
    pub user_context: Arc<UserContext>,
}

impl AuthMiddleware {
    pub fn new(user_context: UserContext) -> Self {
        Self {
            user_context: Arc::new(user_context),
        }
    }

    pub fn validate_access(&self, allowed_roles: &[&str]) -> Result<(), AuthError> {
        validate_roles(&self.user_context, allowed_roles)
    }
}
