use jsonwebtoken::{decode, decode_header, DecodingKey, Validation, Algorithm};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub email: Option<String>,
    pub realm_access: Option<RealmAccess>,
    pub iss: String,
    pub exp: usize,
    pub iat: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RealmAccess {
    pub roles: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum JwtError {
    #[error("failed to fetch JWKS: {0}")]
    FetchFailed(String),

    #[error("invalid JWKS response")]
    InvalidJwksResponse,

    #[error("missing kid in JWT header")]
    MissingKid,

    #[error("signing key not found for kid: {0}")]
    KeyNotFound(String),

    #[error("token validation failed: {0}")]
    ValidationFailed(String),
}

impl From<jsonwebtoken::errors::Error> for JwtError {
    fn from(e: jsonwebtoken::errors::Error) -> Self {
        JwtError::ValidationFailed(e.to_string())
    }
}

#[derive(Debug, Deserialize)]
struct JwksResponse {
    keys: Vec<Jwk>,
}

#[derive(Debug, Deserialize)]
struct Jwk {
    kid: Option<String>,
    #[serde(rename = "use")]
    use_: Option<String>,
    n: Option<String>,
    e: Option<String>,
}

pub struct JwtValidator {
    keys: HashMap<String, DecodingKey>,
    validation: Validation,
}

impl JwtValidator {
    pub async fn new(jwks_url: &str, issuer: &str) -> Result<Self, JwtError> {
        let keys = fetch_keys(jwks_url).await?;

        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[issuer]);
        validation.validate_exp = true;
        validation.validate_aud = false;

        Ok(Self { keys, validation })
    }

    pub fn validate(&self, token: &str) -> Result<Claims, JwtError> {
        let header = decode_header(token)?;
        let kid = header.kid.ok_or(JwtError::MissingKid)?;
        let key = self.keys.get(&kid).ok_or_else(|| JwtError::KeyNotFound(kid))?;
        let token_data = decode::<Claims>(token, key, &self.validation)?;
        Ok(token_data.claims)
    }
}

async fn fetch_keys(jwks_url: &str) -> Result<HashMap<String, DecodingKey>, JwtError> {
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .map_err(|e| JwtError::FetchFailed(e.to_string()))?;

    let response = client
        .get(jwks_url)
        .send()
        .await
        .map_err(|e| JwtError::FetchFailed(e.to_string()))?;

    if !response.status().is_success() {
        return Err(JwtError::FetchFailed(format!(
            "HTTP {} from JWKS endpoint",
            response.status()
        )));
    }

    let jwks: JwksResponse = response
        .json()
        .await
        .map_err(|_| JwtError::InvalidJwksResponse)?;

    let mut keys = HashMap::new();

    for jwk in jwks.keys {
        let kid = match jwk.kid {
            Some(ref k) => k.clone(),
            None => continue,
        };

        let use_ = jwk.use_.as_deref().unwrap_or("sig");
        if use_ != "sig" {
            continue;
        }

        let n = match jwk.n {
            Some(ref n) => n.clone(),
            None => continue,
        };

        let e = match jwk.e {
            Some(ref e) => e.clone(),
            None => continue,
        };

        if let Ok(key) = DecodingKey::from_rsa_components(&n, &e) {
            keys.insert(kid, key);
        }
    }

    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};

    fn create_test_key() -> (String, String, String) {
        ("test-kid".into(), "2m7yCIiJLK-XFA0iF4UqJa38sNH8qHhrZzFKAWMmkZzghm3FAXg5pId1bBsY4lsCKC7sCkJmyubOwkVIGVhK4G_Z5h7Q5fBu3XKs0FfGB5pD62q5qWPoR9JCNmp5jRqKzqJ5M-pP-yXQsFoB6IV_yjY3ITqGDVq-8wjAF_7xlMwSdQC6QdNWVRFs9Wg3J6wvFmn88oItCkB-I7jh1OZFTu2o_Q_LMhfnIP_3fQ8HF7XMiYXXBQ5RY6zO2z42DXIx4D1rQ2HvFrv4ly1Q3S2xGL6KhQ3RSMR7OH0z4LYPDhLfqM9X9Gq0T2hq2zzPE_Pd6e8gCFtFMbQFQIGjTMgLw".into(), "AQAB".into())
    }

    #[test]
    fn test_claims_deserialization() {
        let json = r#"{
            "sub": "550e8400-e29b-41d4-a716-446655440000",
            "email": "test@example.com",
            "realm_access": { "roles": ["driver", "admin"] },
            "iss": "http://localhost:8080/realms/bornemap",
            "exp": 9999999999,
            "iat": 1000000000
        }"#;
        let claims: Claims = serde_json::from_str(json).unwrap();
        assert_eq!(claims.sub, "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(claims.email, Some("test@example.com".into()));
        assert!(claims.realm_access.unwrap().roles.contains(&"admin".into()));
    }

    #[test]
    fn test_claims_no_email() {
        let json = r#"{
            "sub": "550e8400-e29b-41d4-a716-446655440000",
            "iss": "http://localhost:8080/realms/bornemap",
            "exp": 9999999999,
            "iat": 1000000000
        }"#;
        let claims: Claims = serde_json::from_str(json).unwrap();
        assert!(claims.email.is_none());
    }

    #[test]
    fn test_claims_no_realm_access() {
        let json = r#"{
            "sub": "550e8400-e29b-41d4-a716-446655440000",
            "iss": "http://localhost:8080/realms/bornemap",
            "exp": 9999999999,
            "iat": 1000000000
        }"#;
        let claims: Claims = serde_json::from_str(json).unwrap();
        assert!(claims.realm_access.is_none());
    }

    #[test]
    fn test_jwt_error_display() {
        let err = JwtError::MissingKid;
        assert_eq!(err.to_string(), "missing kid in JWT header");

        let err = JwtError::KeyNotFound("abc".into());
        assert_eq!(err.to_string(), "signing key not found for kid: abc");
    }

    #[test]
    fn test_jwt_error_from_jsonwebtoken_error() {
        let jwt_err = jsonwebtoken::errors::ErrorKind::InvalidToken;
        let err: JwtError = jsonwebtoken::errors::Error::from(jwt_err).into();
        assert!(matches!(err, JwtError::ValidationFailed(_)));
    }

    #[test]
    fn test_key_not_found_when_no_matching_kid() {
        let mut keys = HashMap::new();
        let (_kid, n, e) = create_test_key();
        if let Ok(key) = DecodingKey::from_rsa_components(&n, &e) {
            keys.insert("other-kid".into(), key);
        }
        let validation = Validation::new(Algorithm::RS256);
        let validator = JwtValidator { keys, validation };

        let header = Header {
            kid: Some("test-kid".into()),
            alg: Algorithm::RS256,
            ..Default::default()
        };

        let token = encode(&header, &serde_json::json!({"sub": "test"}), &EncodingKey::from_rsa_pem(b"invalid").unwrap()).unwrap_or_default();
        if !token.is_empty() {
            let result = validator.validate(&token);
            assert!(result.is_err());
        }
    }
}
