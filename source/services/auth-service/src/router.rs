use std::sync::Arc;

use axum::{routing::{get, post}, Router};
use axum::http::Method;
use tower_http::cors::{Any, CorsLayer};

use crate::api::{auth_routes, health, profile};
use crate::state::AppState;

pub fn create_router(state: Arc<AppState>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::PUT, Method::OPTIONS])
        .allow_headers(Any);

    Router::new()
        .route("/health", get(health::health_handler))
        .route("/ready", get(health::ready_handler))
        .route("/live", get(health::live_handler))
        .route("/api/v1/profile/me", get(profile::me_get).put(profile::me_put))
        .route("/auth/login/init", get(crate::auth::login::login_init))
        .route("/auth/register/init", get(crate::auth::register::register_init))
        .route("/auth/callback", post(crate::auth::callback::callback_handler))
        .route("/auth/me", get(auth_routes::me_handler))
        .with_state(state)
        .layer(cors)
}
