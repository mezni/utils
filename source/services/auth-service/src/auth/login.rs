use axum::{extract::State, http::StatusCode, Json};
use serde_json::{json, Value};
use std::sync::Arc;

use crate::state::AppState;

pub async fn login_init(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let csrf_state = state.session_manager.create_state().await;
    let authorize_url = state.oidc_client.build_authorize_url(&csrf_state);

    tracing::info!("login init, state={}", csrf_state);
    Ok(Json(json!({ "redirect_url": authorize_url })))
}
