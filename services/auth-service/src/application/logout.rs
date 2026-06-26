use bornemap_core::{AuthError, SessionRepository, UserRepository};
use uuid::Uuid;

fn convert_app_error_to_auth_error(e: bornemap_core::AppError) -> AuthError {
    match e {
        bornemap_core::AppError::Unauthorized => AuthError::Unauthorized,
        bornemap_core::AppError::InvalidCredentials => AuthError::InvalidCredentials,
        bornemap_core::AppError::DatabaseError(_) => AuthError::InternalError,
        bornemap_core::AppError::InternalError => AuthError::InternalError,
        _ => AuthError::InternalError,
    }
}

#[derive(Debug)]
pub struct LogoutRequest {
    pub user_id: String,
}

pub struct LogoutUseCase<R: UserRepository, S: SessionRepository> {
    user_repo: R,
    session_repo: S,
}

impl<R: UserRepository, S: SessionRepository> LogoutUseCase<R, S> {
    pub fn new(user_repo: R, session_repo: S) -> Self {
        Self {
            user_repo,
            session_repo,
        }
    }

    pub async fn execute(&self, req: LogoutRequest) -> Result<(), AuthError> {
        // Validate user exists
        let user_id = Uuid::parse_str(&req.user_id).map_err(|_| AuthError::InvalidCredentials)?;

        let user = self
            .user_repo
            .find_by_id(user_id)
            .await?
            .ok_or(AuthError::UserNotFound)?;

        // Invalidate all user sessions
        self.session_repo
            .delete_user_sessions(user.id)
            .await
            .map_err(convert_app_error_to_auth_error)?;

        Ok(())
    }
}
