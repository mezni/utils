use axum::{extract::State, http::StatusCode, Json};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;


use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct CallbackRequest {
    pub code: String,
    pub state: Option<String>,
}

pub async fn callback_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CallbackRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if let Some(ref state_param) = body.state {
        if !state.session_manager.verify_and_consume_state(state_param).await {
            tracing::warn!("invalid or expired CSRF state");
            return Err((StatusCode::UNAUTHORIZED, Json(json!({"error": "invalid state"}))));
        }
    }

    let token_resp = state
        .oidc_client
        .exchange_code(&body.code)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "code exchange failed");
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "authentication failed"})),
            )
        })?;

    let claims = state
        .keycloak
        .validate(&token_resp.access_token)
        .map_err(|e| {
            tracing::error!(error = %e, "failed to validate exchanged token");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "token validation failed"})),
            )
        })?;

    let user_uuid = Uuid::parse_str(&claims.sub).map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "invalid sub in token"})),
        )
    })?;

    let roles = claims
        .realm_access
        .as_ref()
        .map(|r| r.roles.clone())
        .unwrap_or_default();

    let session = state
        .session_manager
        .create_session(
            token_resp.access_token,
            token_resp.refresh_token,
            user_uuid,
            roles.clone(),
        )
        .await;

    match state
        .profile_service
        .get_or_create(&crate::infrastructure::keycloak::Claims {
            sub: claims.sub.clone(),
            email: claims.email.clone(),
            realm_access: claims.realm_access,
            iss: String::new(),
            exp: 0,
            iat: 0,
        })
        .await
    {
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(error = %e, "profile auto-provisioning warning");
        }
    }

    tracing::info!(
        "session created: id={}, user={}, roles={:?}",
        session.session_id,
        user_uuid,
        roles
    );

    Ok(Json(json!({
        "session_id": session.session_id,
        "access_token": session.access_token,
        "user": {
            "user_uuid": session.user_uuid,
            "roles": session.roles,
        }
    })))
}
