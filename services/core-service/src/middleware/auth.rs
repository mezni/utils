use actix_web::dev::Payload;
use actix_web::error::{ErrorBadRequest, ErrorUnauthorized};
use actix_web::{Error, FromRequest, HttpRequest};
use jsonwebtoken::{decode, encode, Algorithm, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::future::{ready, Ready};

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,        // Subject (user ID)
    pub email: String,      // User email
    pub roles: Vec<String>, // User roles
    pub exp: usize,         // Expiration time
    pub iat: usize,         // Issued at
    pub iss: String,        // Issuer
    pub aud: String,        // Audience
}

#[derive(Debug)]
pub struct AuthInfo {
    pub user_id: String,
    pub email: String,
    pub roles: Vec<String>,
}

impl FromRequest for AuthInfo {
    type Error = Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        // Extract the Authorization header
        let auth_header = req.headers().get("Authorization")
            .and_then(|header| header.to_str().ok())
            .unwrap_or("");

        // Check if the header starts with "Bearer "
        if !auth_header.starts_with("Bearer ") {
            return ready(Err(ErrorUnauthorized("Missing or invalid Bearer token")));
        }

        // Extract the token
        let token = &auth_header[7..];

        // Get the JWT secret from environment
        let jwt_secret = std::env::var("JWT_SECRET")
            .unwrap_or_else(|_| "default-secret".to_string());

        // Decode and validate the token
        let validation = Validation::new(Algorithm::RS256);
        let decoding_key = DecodingKey::from_secret(jwt_secret.as_ref());

        match decode::<Claims>(token, &decoding_key, &validation) {
            Ok(token_data) => {
                let claims = token_data.claims;
                ready(Ok(AuthInfo {
                    user_id: claims.sub,
                    email: claims.email,
                    roles: claims.roles,
                }))
            }
            Err(err) => {
                log::warn!("JWT validation failed: {}", err);
                ready(Err(ErrorUnauthorized("Invalid or expired token")))
            }
        }
    }
}

/// JWT service for token operations
pub struct JwtService {
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    validation: Validation,
}

impl JwtService {
    /// Create a new JWT service
    pub fn new(jwt_secret: &str) -> Self {
        let encoding_key = EncodingKey::from_secret(jwt_secret.as_ref());
        let decoding_key = DecodingKey::from_secret(jwt_secret.as_ref());
        
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&["bornemap"]);
        validation.set_audience(&["bornemap-api"]);
        
        JwtService {
            encoding_key,
            decoding_key,
            validation,
        }
    }

    /// Validate a JWT token and return claims
    pub fn validate_token(&self, token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
        let token_data = decode::<Claims>(token, &self.decoding_key, &self.validation)?;
        Ok(token_data.claims)
    }

    /// Check if user has required role
    pub fn has_role(&self, claims: &Claims, required_role: &str) -> bool {
        claims.roles.contains(&required_role.to_string())
    }

    /// Check if user has any of the required roles
    pub fn has_any_role(&self, claims: &Claims, required_roles: &[&str]) -> bool {
        required_roles.iter().any(|role| claims.roles.contains(&role.to_string()))
    }
}

/// Middleware factory for JWT authentication
pub fn jwt_auth() -> JwtAuth {
    JwtAuth
}

pub struct JwtAuth;

impl<S, B> actix_web::dev::Transform<S, B> for JwtAuth
where
    S: actix_web::dev::Service<
        Request = actix_web::dev::ServiceRequest,
        Response = actix_web::dev::ServiceResponse<B>,
        Error = actix_web::Error,
    >,
    S::Future: 'static,
    B: 'static,
{
    type Response = actix_web::dev::ServiceResponse<B>;
    type Error = actix_web::Error;
    type Transform = JwtAuthMiddleware<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(JwtAuthMiddleware { service }))
    }
}

pub struct JwtAuthMiddleware<S> {
    service: S,
}

impl<S, B> actix_web::dev::Service<actix_web::dev::ServiceRequest> for JwtAuthMiddleware<S>
where
    S: actix_web::dev::Service<
        Request = actix_web::dev::ServiceRequest,
        Response = actix_web::dev::ServiceResponse<B>,
        Error = actix_web::Error,
    >,
    S::Future: 'static,
    B: 'static,
{
    type Response = actix_web::dev::ServiceResponse<B>;
    type Error = actix_web::Error;
    type Future = S::Future;

    fn poll_ready(&self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&self, req: actix_web::dev::ServiceRequest) -> Self::Future {
        // Skip JWT validation for health and metrics endpoints
        let path = req.path();
        if path == "/health/core-service" || path == "/metrics/core-service" {
            return self.service.call(req);
        }

        // Extract the Authorization header
        let auth_header = req.headers().get("Authorization")
            .and_then(|header| header.to_str().ok())
            .unwrap_or("");

        // Check if the header starts with "Bearer "
        if !auth_header.starts_with("Bearer ") {
            return Box::pin(async {
                Err(ErrorUnauthorized("Missing or invalid Bearer token"))
            });
        }

        // Extract the token
        let token = &auth_header[7..];

        // Get the JWT secret from environment
        let jwt_secret = std::env::var("JWT_SECRET")
            .unwrap_or_else(|_| "default-secret".to_string());

        // Decode and validate the token
        let validation = Validation::new(Algorithm::RS256);
        let decoding_key = DecodingKey::from_secret(jwt_secret.as_ref());

        match decode::<Claims>(token, &decoding_key, &validation) {
            Ok(token_data) => {
                // Add user info to request extensions
                let claims = token_data.claims;
                req.extensions_mut().insert(AuthInfo {
                    user_id: claims.sub,
                    email: claims.email,
                    roles: claims.roles,
                });
                
                self.service.call(req)
            }
            Err(err) => {
                log::warn!("JWT validation failed: {}", err);
                Box::pin(async {
                    Err(ErrorUnauthorized("Invalid or expired token"))
                })
            }
        }
    }
}

/// Role-based access control middleware
pub fn require_role(role: &'static str) -> impl actix_web::dev::Transform<
    actix_web::dev::Service<
        Request = actix_web::dev::ServiceRequest,
        Response = actix_web::dev::ServiceResponse,
        Error = actix_web::Error,
    >,
    Error = actix_web::Error,
    InitError = (),
{
    RoleAuth { role }
}

pub struct RoleAuth {
    role: &'static str,
}

impl<S, B> actix_web::dev::Transform<S, B> for RoleAuth
where
    S: actix_web::dev::Service<
        Request = actix_web::dev::ServiceRequest,
        Response = actix_web::dev::ServiceResponse<B>,
        Error = actix_web::Error,
    >,
    S::Future: 'static,
    B: 'static,
{
    type Response = actix_web::dev::ServiceResponse<B>;
    type Error = actix_web::Error;
    type Transform = RoleAuthMiddleware<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(RoleAuthMiddleware {
            service,
            role: self.role,
        }))
    }
}

pub struct RoleAuthMiddleware<S> {
    service: S,
    role: &'static str,
}

impl<S, B> actix_web::dev::Service<actix_web::dev::ServiceRequest> for RoleAuthMiddleware<S>
where
    S: actix_web::dev::Service<
        Request = actix_web::dev::ServiceRequest,
        Response = actix_web::dev::ServiceResponse<B>,
        Error = actix_web::Error,
    >,
    S::Future: 'static,
    B: 'static,
{
    type Response = actix_web::dev::ServiceResponse<B>;
    type Error = actix_web::Error;
    type Future = S::Future;

    fn poll_ready(&self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&self, req: actix_web::dev::ServiceRequest) -> Self::Future {
        // Get user info from request extensions
        if let Some(auth_info) = req.extensions().get::<AuthInfo>() {
            if auth_info.roles.contains(&self.role.to_string()) {
                return self.service.call(req);
            }
        }

        Box::pin(async {
            Err(ErrorUnauthorized(format!("Required role: {}", self.role)))
        })
    }
}