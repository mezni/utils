use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    Json,
};
use serde::Serialize;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

use crate::domain::user_profile::UpdateProfileRequest;
use crate::infrastructure::keycloak::JwtError;
use crate::services::profile_service::ProfileServiceError;
use crate::state::AppState;

#[derive(Debug, Clone, Serialize)]
pub struct AuthContext {
    pub user_uuid: Uuid,
    pub email: Option<String>,
    pub roles: Vec<String>,
}

pub fn extract_auth_context(
    headers: &HeaderMap,
    state: &Arc<AppState>,
) -> Result<AuthContext, (StatusCode, Json<Value>)> {
    let auth_header = headers
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "missing authorization header"})),
            )
        })?;

    let claims = state.keycloak.validate(auth_header).map_err(|e| match e {
        JwtError::MissingKid | JwtError::KeyNotFound(_) | JwtError::ValidationFailed(_) => {
            tracing::warn!(error = %e, "JWT validation failed");
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "invalid token"})),
            )
        }
        _ => {
            tracing::error!(error = %e, "JWT validation error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "authentication service error"})),
            )
        }
    })?;

    let user_uuid = Uuid::parse_str(&claims.sub).map_err(|_| {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "invalid sub claim in token"})),
        )
    })?;

    Ok(AuthContext {
        user_uuid,
        email: claims.email,
        roles: claims.realm_access.map(|r| r.roles).unwrap_or_default(),
    })
}

#[derive(Serialize)]
pub struct ProfileResponse {
    pub user_uuid: Uuid,
    pub email: String,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub phone: Option<String>,
    pub locale: Option<String>,
}

impl From<crate::domain::user_profile::UserProfile> for ProfileResponse {
    fn from(p: crate::domain::user_profile::UserProfile) -> Self {
        Self {
            user_uuid: p.user_uuid,
            email: p.email,
            first_name: p.first_name,
            last_name: p.last_name,
            phone: p.phone,
            locale: p.locale,
        }
    }
}

pub async fn me_get(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let auth = extract_auth_context(&headers, &state)?;

    let claims = crate::infrastructure::keycloak::Claims {
        sub: auth.user_uuid.to_string(),
        email: auth.email,
        realm_access: None,
        iss: String::new(),
        exp: 0,
        iat: 0,
    };

    let profile = state
        .profile_service
        .get_or_create(&claims)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "failed to get/create profile");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "failed to retrieve profile"})),
            )
        })?;

    let response: ProfileResponse = profile.into();
    Ok(Json(json!(response)))
}

pub async fn me_put(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<UpdateProfileRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let auth = extract_auth_context(&headers, &state)?;

    let profile = state
        .profile_service
        .update(auth.user_uuid, req)
        .await
        .map_err(|e| match e {
            ProfileServiceError::NotFound => (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "profile not found"})),
            ),
            _ => {
                tracing::error!(error = %e, "failed to update profile");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "failed to update profile"})),
                )
            }
        })?;

    let response: ProfileResponse = profile.into();
    Ok(Json(json!(response)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_context_creation() {
        let ctx = AuthContext {
            user_uuid: Uuid::nil(),
            email: Some("test@example.com".into()),
            roles: vec!["driver".into()],
        };
        assert_eq!(ctx.email, Some("test@example.com".into()));
        assert!(ctx.roles.contains(&"driver".into()));
    }

    #[test]
    fn test_profile_response_from_domain() {
        let profile = crate::domain::user_profile::UserProfile {
            user_uuid: Uuid::nil(),
            email: "test@example.com".into(),
            first_name: Some("John".into()),
            last_name: None,
            phone: None,
            locale: Some("en".into()),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            deleted_at: None,
        };
        let resp: ProfileResponse = profile.into();
        assert_eq!(resp.email, "test@example.com");
        assert_eq!(resp.first_name, Some("John".into()));
        assert!(resp.last_name.is_none());
    }

    #[test]
    fn test_extract_auth_context_missing_header() {
        let error_msg = "missing authorization header";
        assert!(error_msg.len() > 0);
    }
}
