use actix_web::{web, HttpRequest, HttpResponse};
use sqlx::PgPool;
use crate::domain::{RegisterRequest, LoginRequest};
use crate::error::AuthServiceError;
use crate::middleware_auth::extract_token_from_header;

/// Register a new user account
