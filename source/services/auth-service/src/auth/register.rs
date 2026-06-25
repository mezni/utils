use axum::{extract::State, http::StatusCode, Json};
use serde_json::{json, Value};
use std::sync::Arc;

use crate::state::AppState;

pub async fn register_init(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let csrf_state = state.session_manager.create_state().await;
    let register_url = state.oidc_client.build_registration_url(&csrf_state);

    tracing::info!("register init, state={}", csrf_state);
    Ok(Json(json!({ "redirect_url": register_url })))
}
