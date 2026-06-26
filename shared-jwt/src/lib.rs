use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use sha2::{Digest, Sha256};
use shared_contracts::{JwtClaims, UserWithoutSensitive};
use std::collections::HashMap;
use tracing::{error, info, warn};

const JWT_ISSUER: &str = "borne-map-auth";
const JWT_AUDIENCE: &str = "borne-map-api";

pub struct JwtService {
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
}

impl JwtService {
    /// Create a new JWT service with secret key
    pub fn new(secret: &str) -> Result<Self, jsonwebtoken::errors::Error> {
        let encoding_key = EncodingKey::from_secret(secret.as_ref());
        let decoding_key = DecodingKey::from_secret(secret.as_ref());

        info!("JWT service initialized with Ed25519 algorithm");

        Ok(JwtService {
            encoding_key,
            decoding_key,
        })
    }

    /// Create a new JWT service from environment variable
    pub fn from_env() -> Result<Self, jsonwebtoken::errors::Error> {
        let secret =
            std::env::var("JWT_SECRET").expect("JWT_SECRET environment variable must be set");

        Self::new(&secret)
    }

    /// Generate JWT claims for a user
    pub fn generate_claims(
        &self,
        user: &UserWithoutSensitive,
        expires_in_minutes: i64,
    ) -> Result<JwtClaims, jsonwebtoken::errors::Error> {
        let now = Utc::now();
        let expires_at = now + Duration::minutes(expires_in_minutes);

        Ok(JwtClaims {
            sub: user.id.to_string(),
            iss: JWT_ISSUER.to_string(),
            aud: JWT_AUDIENCE.to_string(),
            exp: expires_at.timestamp(),
            iat: now.timestamp(),
            jti: uuid::Uuid::new_v4(),
            email: user.email.clone(),
            user_id: user.id.to_string(),
            status: user.status.clone(),
            email_verified: user.email_verified,
            permissions: vec![], // Start empty for future expansion
        })
    }

    /// Sign a token
    pub fn sign(&self, claims: &JwtClaims) -> Result<String, jsonwebtoken::errors::Error> {
        let header = Header::default();
        encode(&header, &claims, &self.encoding_key)
    }

    /// Verify and decode a token
    pub fn verify(&self, token: &str) -> Result<JwtClaims, jsonwebtoken::errors::Error> {
        let mut validation = Validation::new(jsonwebtoken::Algorithm::EdDSA);

        // Add issuer and audience validation
        validation.set_issuer(&[JWT_ISSUER]);
        validation.set_audience(&[JWT_AUDIENCE]);

        let token_data = decode::<JwtClaims>(token, &self.decoding_key, &validation)?;
        Ok(token_data.claims)
    }

    /// Validate token signature and claims
    pub fn validate(&self, token: &str) -> Result<JwtClaims, jsonwebtoken::errors::Error> {
        let claims = self.verify(token)?;

        // Check if token is expired
        if claims.exp < Utc::now().timestamp() {
            warn!("Token expired");
            return Err(jsonwebtoken::errors::ErrorKind::ExpiredSignature.into());
        }

        info!("Token validated successfully");
        Ok(claims)
    }

    /// Generate JWKS (JSON Web Key Set) endpoint data
    pub fn generate_jwks(&self) -> Result<serde_json::Value, jsonwebtoken::errors::Error> {
        let claims = self.generate_claims(
            &UserWithoutSensitive {
                id: uuid::Uuid::new_v4(),
                email: "dummy@example.com".to_string(),
                email_verified: true,
                status: "active".to_string(),
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            5,
        )?;

        let public_key = self.encoding_key.public_key();

        let jwks = serde_json::json!({
            "keys": [
                {
                    "kty": "OKP",
                    "crv": "Ed25519",
                    "kid": "borne-map-key-1",
                    "x": public_key.encode_hex(),
                }
            ]
        });

        info!("Generated JWKS endpoint data");
        Ok(jwks)
    }

    /// Generate OpenID configuration endpoint data
    pub fn generate_openid_config(&self) -> Result<serde_json::Value, jsonwebtoken::errors::Error> {
        let jwks_uri = format!(
            "{}/.well-known/jwks.json",
            std::env::var("APP_BASE_URL").unwrap_or_else(|_| "http://localhost:8080".to_string())
        );
        let issuer = format!(
            "{}/",
            std::env::var("APP_BASE_URL").unwrap_or_else(|_| "http://localhost:8080".to_string())
        );
        let token_endpoint = format!(
            "{}/auth/login",
            std::env::var("APP_BASE_URL").unwrap_or_else(|_| "http://localhost:8080".to_string())
        );

        let openid_config = serde_json::json!({
            "issuer": issuer,
            "authorization_endpoint": "",
            "token_endpoint": token_endpoint,
            "jwks_uri": jwks_uri,
            "response_types_supported": ["code", "token", "id_token"],
            "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["EdDSA"],
            "scopes_supported": ["openid", "profile", "email"],
            "claims_supported": ["sub", "email", "email_verified", "name", "picture"],
        });

        info!("Generated OpenID configuration endpoint data");
        Ok(openid_config)
    }

    /// Generate token hash for storage (SHA-256)
    pub fn hash_token(&self, token: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(token.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// Verify token hash matches token
    pub fn verify_token_hash(&self, token: &str, hash: &str) -> bool {
        let token_hash = self.hash_token(token);
        token_hash == hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jwt_service_creation() {
        let service = JwtService::new("test-secret-key").unwrap();
        assert_eq!(service.encoding_key, service.decoding_key);
    }

    #[test]
    fn test_token_signing_and_verification() {
        let service = JwtService::new("test-secret-key").unwrap();

        let claims = JwtClaims {
            sub: "user123".to_string(),
            iss: JWT_ISSUER.to_string(),
            aud: JWT_AUDIENCE.to_string(),
            exp: (Utc::now() + Duration::minutes(5)).timestamp(),
            iat: Utc::now().timestamp(),
            jti: uuid::Uuid::new_v4(),
            email: "test@example.com".to_string(),
            user_id: "user123".to_string(),
            status: "active".to_string(),
            email_verified: true,
            permissions: vec![],
        };

        let token = service.sign(&claims).unwrap();
        let decoded = service.verify(&token).unwrap();

        assert_eq!(decoded.sub, claims.sub);
        assert_eq!(decoded.email, claims.email);
    }

    #[test]
    fn test_token_hashing() {
        let service = JwtService::new("test-secret-key").unwrap();

        let token = "test-refresh-token-123";
        let hash = service.hash_token(token);

        assert_ne!(hash, token);
        assert!(service.verify_token_hash(token, &hash));
    }
}
