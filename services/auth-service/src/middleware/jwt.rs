use actix_web::{dev::ServiceRequest, Error, HttpMessage};
use actix_web::error::ErrorUnauthorized;
use domain_types::jwt::{JwtClaims, KeycloakJwtPayload};
use jsonwebtoken::{decode, DecodingKey, Validation, Algorithm};
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct JwtConfig {
    pub jwks_uri: String,
    pub issuer: String,
    pub audience: String,
    pub clock_skew_secs: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JwkSet {
    pub keys: Vec<Jwk>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Jwk {
    pub kid: Option<String>,
    pub kty: Option<String>,
    pub alg: Option<String>,
    pub n: Option<String>,
    pub e: Option<String>,
    pub x5c: Option<Vec<String>>,
    pub use_: Option<String>,
}

#[derive(Clone)]
pub struct JwtMiddleware {
    config: JwtConfig,
    client: Client,
    jwks_cache: Arc<RwLock<HashMap<String, DecodingKey>>>,
    kid_map: Arc<RwLock<HashMap<String, String>>>, // kid -> key_id in cache
}

impl JwtMiddleware {
    pub fn new(config: JwtConfig) -> Self {
        Self {
            config,
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .expect("Failed to create HTTP client"),
            jwks_cache: Arc::new(RwLock::new(HashMap::new())),
            kid_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn fetch_jwks(&self) -> Result<JwkSet, String> {
        let resp = self
            .client
            .get(&self.config.jwks_uri)
            .send()
            .await
            .map_err(|e| format!("JWKS fetch failed: {}", e))?;

        let text = resp
            .text()
            .await
            .map_err(|e| format!("JWKS text read failed: {}", e))?;

        let jwks: JwkSet = serde_json::from_str(&text)
            .map_err(|e| format!("JWKS parse failed: {}", e))?;

        Ok(jwks)
    }

    pub async fn refresh_cache(&self) -> Result<(), String> {
        let jwks = self.fetch_jwks().await?;
        let mut cache = self.jwks_cache.write().await;
        let mut kid_map = self.kid_map.write().await;

        cache.clear();
        kid_map.clear();

        for key in &jwks.keys {
            if let Some(ref kid) = key.kid {
                if let Some(ref x5c) = key.x5c {
                    if !x5c.is_empty() {
                        let pem = format!(
                            "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----",
                            x5c[0]
                        );
                        if let Ok(dk) = DecodingKey::from_rsa_pem(pem.as_bytes()) {
                            cache.insert(kid.clone(), dk);
                            kid_map.insert(kid.clone(), kid.clone());
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn ensure_cache(&self) -> Result<(), String> {
        let cache = self.jwks_cache.read().await;
        if cache.is_empty() {
            drop(cache);
            self.refresh_cache().await?;
        }
        Ok(())
    }

    pub async fn validate_token(&self, token: &str) -> Result<JwtClaims, String> {
        self.ensure_cache().await?;

        let header = jsonwebtoken::decode_header(token)
            .map_err(|e| format!("Invalid token header: {}", e))?;

        let kid = header.kid.clone().unwrap_or_default();
        let cache = self.jwks_cache.read().await;

        let decoding_key = cache.get(&kid).ok_or_else(|| {
            format!("Unknown kid: {}, cache refresh needed", kid)
        })?;

        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[self.config.issuer.clone()]);
        validation.set_audience(&[self.config.audience.clone()]);
        validation.validate_exp = true;
        validation.validate_nbf = true;

        let token_data = decode::<KeycloakJwtPayload>(token, decoding_key, &validation)
            .map_err(|e| format!("Token validation failed: {}", e))?;

        let claims = JwtClaims::try_from(token_data.claims)?;

        Ok(claims)
    }
}

pub async fn jwt_middleware(
    req: ServiceRequest,
    middleware: &JwtMiddleware,
) -> Result<ServiceRequest, Error> {
    let auth_header = req
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or_else(|| ErrorUnauthorized("Missing or invalid Authorization header"))?;

    let claims = middleware
        .validate_token(auth_header)
        .await
        .map_err(|e| ErrorUnauthorized(e))?;

    req.extensions_mut().insert(claims);
    Ok(req)
}
