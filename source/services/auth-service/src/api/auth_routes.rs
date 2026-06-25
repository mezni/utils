use axum::{extract::State, http::StatusCode, Json};
use serde_json::{json, Value};
use std::sync::Arc;

use crate::state::AppState;

pub async fn me_handler(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let session_id = headers
        .get("X-Session-Id")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<uuid::Uuid>().ok())
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "missing X-Session-Id header"})),
            )
        })?;

    let session = state
        .session_manager
        .get_session(session_id)
        .await
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "session not found or expired"})),
            )
        })?;

    let profile = state
        .profile_service
        .get(session.user_uuid)
        .await
        .map_err(|_| {
            (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "profile not found"})),
            )
        })?;

    Ok(Json(json!({
        "session_id": session.session_id,
        "user": {
            "user_uuid": session.user_uuid,
            "email": profile.email,
            "roles": session.roles,
        }
    })))
}
