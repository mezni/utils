use actix_web::{web, HttpResponse, Result};
use bornemap_core::AppError;
use bornemap_auth::OAuthStateStore;
use crate::infrastructure::{PgOAuthRepository, GoogleOAuthProvider, OAuthStartUseCase, OAuthCallbackUseCase};
use crate::infrastructure::oauth_repository::PgOAuthRepository as OAuthRepository;
use crate::response::ApiResponse;
use tracing::{error, info};

#[derive(Clone)]
pub struct OAuthState {
    pub google_provider: Option<GoogleOAuthProvider>,
    pub state_store: Box<dyn OAuthStateStore>,
    pub oauth_repository: OAuthRepository,
}

pub fn configure_oauth_routes(cfg: &mut web::ServiceConfig, oauth_state: OAuthState) {
    cfg.service(
        web::resource("/oauth/{provider}/start")
            .route(web::get().to(oauth_start))
    )
    .service(
        web::resource("/oauth/{provider}/callback")
            .route(web::get().to(oauth_callback))
    );
}

async fn oauth_start(
    path: web::Path<(String,)>,
    query: web::Query<OAuthStartQuery>,
    state: web::Data<OAuthState>,
) -> Result<HttpResponse> {
    let (provider_name,) = path.into_inner();
    let redirect_uri = query.redirect_uri.clone().unwrap_or_else(|| {
        format!("http://localhost:8080/api/v1/auth/oauth/{}/callback", provider_name)
    });

    match provider_name.as_str() {
        "google" => {
            if let Some(google_provider) = &state.google_provider {
                let use_case = OAuthStartUseCase::new(
                    google_provider.clone(),
                    state.state_store.clone()
                );

                match use_case.execute(&redirect_uri).await {
                    Ok(auth_url) => {
                        info!("OAuth start successful for provider: {}", provider_name);
                        Ok(HttpResponse::Found()
                            .header("Location", auth_url)
                            .finish())
                    }
                    Err(e) => {
                        error!("OAuth start failed for provider {}: {}", provider_name, e);
                        Ok(HttpResponse::BadRequest()
                            .json(ApiResponse::error(
                                AppError::ValidationError(format!("OAuth start failed: {}", e))
                            )))
                    }
                }
            } else {
                error!("Google OAuth provider not configured");
                Ok(HttpResponse::BadRequest()
                    .json(ApiResponse::error(
                        AppError::UnsupportedOAuthProvider("Google OAuth provider not configured".to_string())
                    )))
            }
        }
        _ => {
            error!("Unsupported OAuth provider: {}", provider_name);
            Ok(HttpResponse::BadRequest()
                .json(ApiResponse::error(
                    AppError::UnsupportedOAuthProvider(format!("Unsupported OAuth provider: {}", provider_name))
                )))
        }
    }
}

async fn oauth_callback(
    path: web::Path<(String,)>,
    query: web::Query<OAuthCallbackQuery>,
    state: web::Data<OAuthState>,
) -> Result<HttpResponse> {
    let (provider_name,) = path.into_inner();
    let code = query.code.clone().ok_or_else(|| {
        AppError::ValidationError("Authorization code is required".to_string())
    })?;
    let state_param = query.state.clone().ok_or_else(|| {
        AppError::ValidationError("State parameter is required".to_string())
    })?;

    let redirect_uri = format!("http://localhost:8080/api/v1/auth/oauth/{}/callback", provider_name);

    match provider_name.as_str() {
        "google" => {
            if let Some(google_provider) = &state.google_provider {
                let use_case = OAuthCallbackUseCase::new(
                    google_provider.clone(),
                    state.state_store.clone(),
                    state.oauth_repository.clone()
                );

                match use_case.execute(code, state_param, &redirect_uri).await {
                    Ok(user) => {
                        info!("OAuth callback successful for user: {}", user.id);
                        // In a real implementation, you would generate JWT tokens here
                        // For now, return a success response with user info
                        Ok(HttpResponse::Ok()
                            .json(ApiResponse::success(Some(serde_json::json!({
                                "user_id": user.id,
                                "email": user.email,
                                "role": user.role.as_str(),
                                "status": user.status.as_str(),
                            })))))
                    }
                    Err(e) => {
                        error!("OAuth callback failed for provider {}: {}", provider_name, e);
                        Ok(HttpResponse::BadRequest()
                            .json(ApiResponse::error(e)))
                    }
                }
            } else {
                error!("Google OAuth provider not configured");
                Ok(HttpResponse::BadRequest()
                    .json(ApiResponse::error(
                        AppError::UnsupportedOAuthProvider("Google OAuth provider not configured".to_string())
                    )))
            }
        }
        _ => {
            error!("Unsupported OAuth provider: {}", provider_name);
            Ok(HttpResponse::BadRequest()
                .json(ApiResponse::error(
                    AppError::UnsupportedOAuthProvider(format!("Unsupported OAuth provider: {}", provider_name))
                )))
        }
    }
}

#[derive(serde::Deserialize)]
struct OAuthStartQuery {
    redirect_uri: Option<String>,
}

#[derive(serde::Deserialize)]
struct OAuthCallbackQuery {
    code: Option<String>,
    state: Option<String>,
}