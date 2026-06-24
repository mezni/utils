use std::sync::Arc;

use bornemap_platform_core::error::AppResult;
use bornemap_platform_core::result::KpiData;
use bornemap_platform_db::traits::DashboardRepository;

pub struct DashboardService {
    repo: Arc<dyn DashboardRepository>,
}

impl DashboardService {
    pub fn new(repo: Arc<dyn DashboardRepository>) -> Self {
        Self { repo }
    }

    pub async fn get_kpis(&self) -> AppResult<KpiData> {
        let (partners, stations, chargers) = self.repo.get_kpis().await?;
        Ok(KpiData {
            partners_count: partners,
            stations_count: stations,
            chargers_count: chargers,
        })
    }
}
