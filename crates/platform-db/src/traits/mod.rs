use async_trait::async_trait;
use bornemap_platform_core::error::AppResult;
use bornemap_platform_core::models::{Charger, Partner, Station};
use bornemap_platform_core::pagination::Pagination;

#[async_trait]
pub trait PartnerRepository: Send + Sync {
    async fn create(&self, name: &str, created_by: &str, updated_by: &str) -> AppResult<Partner>;
    async fn get_by_id(&self, id: &str) -> AppResult<Option<Partner>>;
    async fn list(&self, page: u32, limit: u32) -> AppResult<(Vec<Partner>, u64)>;
    async fn update(&self, id: &str, name: &str, updated_by: &str) -> AppResult<Partner>;
    async fn hard_delete(&self, id: &str) -> AppResult<()>;
    async fn soft_delete(&self, id: &str, updated_by: &str) -> AppResult<()>;
    async fn undelete(&self, id: &str, updated_by: &str) -> AppResult<Partner>;
}

#[async_trait]
pub trait StationRepository: Send + Sync {
    async fn create(
        &self,
        name: &str,
        location: Option<&str>,
        partner_id: &str,
        created_by: &str,
        updated_by: &str,
    ) -> AppResult<Station>;
    async fn get_by_id(&self, id: &str) -> AppResult<Option<Station>>;
    async fn list(&self, page: u32, limit: u32, partner_id: Option<&str>) -> AppResult<(Vec<Station>, u64)>;
    async fn update(&self, id: &str, name: &str, location: Option<&str>, updated_by: &str) -> AppResult<Station>;
    async fn hard_delete(&self, id: &str) -> AppResult<()>;
    async fn soft_delete(&self, id: &str, updated_by: &str) -> AppResult<()>;
    async fn undelete(&self, id: &str, updated_by: &str) -> AppResult<Station>;
}

#[async_trait]
pub trait ChargerRepository: Send + Sync {
    async fn create(
        &self,
        station_id: &str,
        status: &str,
        power_rating: i32,
        created_by: &str,
        updated_by: &str,
    ) -> AppResult<Charger>;
    async fn get_by_id(&self, id: &str) -> AppResult<Option<Charger>>;
    async fn list(&self, page: u32, limit: u32, station_id: Option<&str>) -> AppResult<(Vec<Charger>, u64)>;
    async fn update_status(&self, id: &str, status: &str, updated_by: &str) -> AppResult<Charger>;
    async fn update_power_rating(&self, id: &str, power_rating: i32, updated_by: &str) -> AppResult<Charger>;
    async fn hard_delete(&self, id: &str) -> AppResult<()>;
    async fn soft_delete(&self, id: &str, updated_by: &str) -> AppResult<()>;
    async fn undelete(&self, id: &str, updated_by: &str) -> AppResult<Charger>;
}

#[async_trait]
pub trait DashboardRepository: Send + Sync {
    async fn get_kpis(&self) -> AppResult<(i64, i64, i64)>;
}
