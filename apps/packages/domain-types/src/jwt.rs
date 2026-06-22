use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::role::Role;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtClaims {
    pub sub: Uuid,
    pub email: Option<String>,
    #[serde(default)]
    pub role: Role,
    pub exp: i64,
    pub iat: i64,
}

impl JwtClaims {
    pub fn is_expired(&self, now_epoch: i64, clock_skew_secs: i64) -> bool {
        (self.exp + clock_skew_secs) < now_epoch
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealmAccess {
    pub roles: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeycloakJwtPayload {
    pub sub: String,
    pub email: Option<String>,
    pub preferred_username: Option<String>,
    pub exp: i64,
    pub iat: i64,
    pub iss: String,
    pub aud: String,
    #[serde(rename = "realm_access")]
    pub realm_access: Option<RealmAccess>,
    #[serde(rename = "bornemap_role")]
    pub bornemap_role: Option<String>,
}

impl TryFrom<KeycloakJwtPayload> for JwtClaims {
    type Error = String;

    fn try_from(payload: KeycloakJwtPayload) -> Result<Self, Self::Error> {
        let sub = Uuid::parse_str(&payload.sub)
            .map_err(|_| format!("Invalid UUID in sub claim: {}", payload.sub))?;

        let role = payload
            .bornemap_role
            .as_deref()
            .and_then(Role::from_str)
            .or_else(|| {
                payload
                    .realm_access
                    .as_ref()
                    .and_then(|ra| {
                        ra.roles
                            .iter()
                            .find_map(|r| Role::from_str(r))
                    })
            })
            .unwrap_or_default();

        Ok(JwtClaims {
            sub,
            email: payload.email,
            role,
            exp: payload.exp,
            iat: payload.iat,
        })
    }
}
