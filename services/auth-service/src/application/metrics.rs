use bornemap_core::{AppError, MetricsRange, UserRepository, UsersMetrics};

pub struct GetUsersMetricsRequest {
    pub range: MetricsRange,
}

pub struct GetUsersMetricsUseCase<R: UserRepository> {
    repo: R,
}

impl<R: UserRepository> GetUsersMetricsUseCase<R> {
    pub fn new(repo: R) -> Self {
        Self { repo }
    }

    pub async fn execute(&self, req: GetUsersMetricsRequest) -> Result<UsersMetrics, AppError> {
        let total = self.repo.count_users().await?;
        let growth = self.repo.users_growth_by_day(&req.range).await?;
        Ok(UsersMetrics { total, growth })
    }
}
