use crate::errors::AuthError;
use chrono::{TimeDelta, Utc};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// JWT Claims
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtClaims {
    pub sub: String,
    pub iss: String,
    pub aud: serde_json::Value,
    pub exp: usize,
    pub iat: usize,
    pub email: Option<String>,
    #[serde(rename = "realm_access")]
    pub realm_access: Option<RealmAccess>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealmAccess {
    pub roles: Vec<String>,
}

impl JwtClaims {
    pub fn role(&self) -> Option<&str> {
        let valid = ["registered_driver", "partner", "admin"];
        self.realm_access
            .as_ref()
            .and_then(|r| r.roles.iter().find(|r| valid.contains(&r.as_str())))
            .map(|s| s.as_str())
    }
}

// ---------------------------------------------------------------------------
// JWK types (subset of JWKS spec)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Jwk {
    pub kid: String,
    pub kty: String,
    pub alg: String,
    pub n: String,
    pub e: String,
    #[serde(rename = "use")]
    pub use_: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwksResponse {
    pub keys: Vec<Jwk>,
}

// ---------------------------------------------------------------------------
// JWKS Cache
// ---------------------------------------------------------------------------

struct CachedKey {
    key: DecodingKey,
    algorithm: Algorithm,
    fetched_at: chrono::DateTime<Utc>,
}

pub struct JwksCache {
    keys: RwLock<HashMap<String, CachedKey>>,
    jwks_url: String,
    ttl_seconds: i64,
}

impl JwksCache {
    pub fn new(jwks_url: String) -> Self {
        Self {
            keys: RwLock::new(HashMap::new()),
            jwks_url,
            ttl_seconds: 3600,
        }
    }

    pub async fn get(&self, kid: &str) -> Result<(DecodingKey, Algorithm), AuthError> {
        // Check cache for a non-expired key
        {
            let keys = self.keys.read().await;
            if let Some(cached) = keys.get(kid) {
                let age = Utc::now() - cached.fetched_at;
                if age < TimeDelta::seconds(self.ttl_seconds) {
                    return Ok((cached.key.clone(), cached.algorithm));
                }
            }
        }

        // Cache miss or expired — fetch JWKS
        self.refresh().await?;

        // Retry lookup
        let keys = self.keys.read().await;
        match keys.get(kid) {
            Some(cached) => Ok((cached.key.clone(), cached.algorithm)),
            None => Err(AuthError::ValidationError(
                format!("No JWK found for kid: {}", kid),
            )),
        }
    }

    pub async fn refresh(&self) -> Result<(), AuthError> {
        let resp: JwksResponse = reqwest::get(&self.jwks_url)
            .await
            .map_err(|e| AuthError::JwksFetchError(e.to_string()))?
            .json()
            .await
            .map_err(|e| AuthError::JwksFetchError(e.to_string()))?;

        let now = Utc::now();
        let mut keys = self.keys.write().await;

        for jwk in resp.keys {
            if jwk.use_.as_deref() != Some("sig") {
                continue;
            }
            let algorithm = match jwk.alg.as_str() {
                "RS256" => Algorithm::RS256,
                "RS384" => Algorithm::RS384,
                "RS512" => Algorithm::RS512,
                other => {
                    warn!("Unsupported JWK algorithm: {}", other);
                    continue;
                }
            };
            let decoding_key = DecodingKey::from_rsa_components(&jwk.n, &jwk.e)
                .map_err(|e| AuthError::JwksFetchError(format!("Invalid JWK: {}", e)))?;

            keys.insert(
                jwk.kid.clone(),
                CachedKey {
                    key: decoding_key,
                    algorithm,
                    fetched_at: now,
                },
            );
        }

        info!("JWKS cache refreshed — {} keys loaded", keys.len());
        Ok(())
    }

    /// Degraded-mode validation: use any cached key (even stale) when JWKS is unreachable.
    pub async fn validate_degraded(&self, kid: &str) -> Result<(DecodingKey, Algorithm), AuthError> {
        let keys = self.keys.read().await;
        match keys.get(kid) {
            Some(cached) => Ok((cached.key.clone(), cached.algorithm)),
            None => Err(AuthError::Unauthenticated),
        }
    }
}

// ---------------------------------------------------------------------------
// Public validation function
// ---------------------------------------------------------------------------

static JWKS_CACHE: Lazy<RwLock<Option<Arc<JwksCache>>>> = Lazy::new(|| RwLock::new(None));

pub async fn init_jwks_cache(jwks_url: String) {
    let cache = Arc::new(JwksCache::new(jwks_url));
    // Warm the cache
    if let Err(e) = cache.refresh().await {
        warn!("Initial JWKS fetch failed (will retry on first request): {}", e);
    }
    let mut guard = JWKS_CACHE.write().await;
    *guard = Some(cache);
}

pub async fn validate_token(
    token: &str,
    issuer: &str,
    audience: &str,
) -> Result<JwtClaims, AuthError> {
    let guard = JWKS_CACHE.read().await;
    let cache = guard
        .as_ref()
        .ok_or_else(|| AuthError::ValidationError("JWKS cache not initialized".into()))?;

    // Extract kid from token header
    let header = decode_header(token)
        .map_err(|_| AuthError::ValidationError("Invalid token header".into()))?;
    let kid = header
        .kid
        .ok_or_else(|| AuthError::ValidationError("Missing kid in token header".into()))?;

    // Get key: try normal cache first, fall back to degraded mode
    let (key, alg) = match cache.get(&kid).await {
        Ok(k) => k,
        Err(AuthError::JwksFetchError(_)) => {
            warn!("JWKS unreachable — using stale cached key (degraded mode)");
            cache
                .validate_degraded(&kid)
                .await
                .map_err(|_| AuthError::Unauthenticated)?
        }
        Err(e) => return Err(e),
    };

    // Build validation
    let mut validation = Validation::new(alg);
    validation.set_issuer(&[issuer]);
    validation.set_audience(&[audience]);
    validation.validate_exp = true;

    let token_data = decode::<JwtClaims>(token, &key, &validation)
        .map_err(|e| {
            match e.kind() {
                jsonwebtoken::errors::ErrorKind::ExpiredSignature => AuthError::TokenExpired,
                _ => AuthError::ValidationError(e.to_string()),
            }
        })?;

    Ok(token_data.claims)
}
