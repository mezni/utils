use uuid::Uuid;

use crate::domain::user_profile::{UpdateProfileRequest, UserProfile};
use crate::infrastructure::keycloak::Claims;
use crate::repository::user_profile_repository::{RepositoryError, UserProfileRepository};

#[derive(Debug, thiserror::Error)]
pub enum ProfileServiceError {
    #[error("not found")]
    NotFound,

    #[error("database error: {0}")]
    Database(String),
}

impl From<RepositoryError> for ProfileServiceError {
    fn from(e: RepositoryError) -> Self {
        match e {
            RepositoryError::NotFound => ProfileServiceError::NotFound,
            RepositoryError::Database(msg) => ProfileServiceError::Database(msg),
        }
    }
}

pub struct ProfileService {
    repo: UserProfileRepository,
}

impl ProfileService {
    pub fn new(repo: UserProfileRepository) -> Self {
        Self { repo }
    }

    pub async fn get_or_create(&self, claims: &Claims) -> Result<UserProfile, ProfileServiceError> {
        let user_uuid = Uuid::parse_str(&claims.sub)
            .map_err(|e| ProfileServiceError::Database(format!("invalid sub uuid: {}", e)))?;

        match self.repo.find_by_uuid(user_uuid).await {
            Ok(profile) => Ok(profile),
            Err(RepositoryError::NotFound) => {
                let email = claims.email.as_deref().unwrap_or(&claims.sub);
                self.repo
                    .insert(user_uuid, email, None, None)
                    .await
                    .map_err(Into::into)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub async fn get(&self, user_uuid: Uuid) -> Result<UserProfile, ProfileServiceError> {
        self.repo.find_by_uuid(user_uuid).await.map_err(Into::into)
    }

    pub async fn update(
        &self,
        user_uuid: Uuid,
        req: UpdateProfileRequest,
    ) -> Result<UserProfile, ProfileServiceError> {
        self.repo
            .update(
                user_uuid,
                req.first_name.as_deref(),
                req.last_name.as_deref(),
                req.phone.as_deref(),
                req.locale.as_deref(),
            )
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_profile_service_error_display() {
        assert_eq!(ProfileServiceError::NotFound.to_string(), "not found");
        assert_eq!(
            ProfileServiceError::Database("conn failed".into()).to_string(),
            "database error: conn failed"
        );
    }

    #[test]
    fn test_profile_service_error_from_repository_error() {
        let err: ProfileServiceError = RepositoryError::NotFound.into();
        assert!(matches!(err, ProfileServiceError::NotFound));

        let err: ProfileServiceError = RepositoryError::Database("err".into()).into();
        assert!(matches!(err, ProfileServiceError::Database(_)));
    }
}
