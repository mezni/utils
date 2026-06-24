use std::sync::Arc;

use bornemap_platform_core::constants::{DEFAULT_PAGE_SIZE, MAX_POWER_RATING, MIN_POWER_RATING, MIN_PAGE_SIZE};
use bornemap_platform_core::error::AppResult;
use bornemap_platform_core::models::Charger;
use bornemap_platform_db::traits::{ChargerRepository, StationRepository};

pub struct ChargerService {
    repo: Arc<dyn ChargerRepository>,
    station_repo: Arc<dyn StationRepository>,
}

impl ChargerService {
    pub fn new(repo: Arc<dyn ChargerRepository>, station_repo: Arc<dyn StationRepository>) -> Self {
        Self { repo, station_repo }
    }

    pub async fn create(
        &self,
        station_id: &str,
        status: &str,
        power_rating: i32,
        created_by: &str,
    ) -> AppResult<Charger> {
        if !matches!(status, "ACTIVE" | "INACTIVE" | "MAINTENANCE" | "DISABLED") {
            return Err(AppError::Validation(
                "Invalid status. Must be: ACTIVE, INACTIVE, MAINTENANCE, or DISABLED".into(),
            ));
        }
        if power_rating < MIN_POWER_RATING || power_rating > MAX_POWER_RATING {
            return Err(AppError::Validation(
                format!("Power rating must be between {} and {} kW", MIN_POWER_RATING, MAX_POWER_RATING).into(),
            ));
        }
        let station = self.station_repo.get_by_id(station_id).await?;
        match station {
            Some(s) if s.is_active() => {}
            _ => return Err(AppError::NotFound(format!("Station {} not found or inactive", station_id))),
        }
        self.repo.create(station_id, status, power_rating, created_by, created_by).await
    }

    pub async fn get(&self, id: &str) -> AppResult<Charger> {
        let charger = self.repo.get_by_id(id).await?;
        match charger {
            Some(c) if c.is_active() => Ok(c),
            _ => Err(AppError::NotFound(format!("Charger {} not found", id))),
        }
    }

    pub async fn list(&self, page: u32, limit: u32, station_id: Option<&str>) -> AppResult<(Vec<Charger>, u64)> {
        let page = page.clamp(MIN_PAGE_SIZE, u32::MAX);
        let limit = limit.clamp(MIN_PAGE_SIZE, MAX_PAGE_SIZE);
        self.repo.list(page, limit, station_id).await
    }

    pub async fn update_status(&self, id: &str, status: &str, updated_by: &str) -> AppResult<Charger> {
        if !matches!(status, "ACTIVE" | "INACTIVE" | "MAINTENANCE" | "DISABLED") {
            return Err(AppError::Validation(
                "Invalid status. Must be: ACTIVE, INACTIVE, MAINTENANCE, or DISABLED".into(),
            ));
        }
        let charger = self.repo.get_by_id(id).await?;
        match charger {
            Some(c) if c.is_active() => self.repo.update_status(id, status, updated_by).await,
            Some(_) => Err(AppError::Validation("Charger is deleted".into())),
            None => Err(AppError::NotFound(format!("Charger {} not found", id))),
        }
    }

    pub async fn update_power_rating(&self, id: &str, power_rating: i32, updated_by: &str) -> AppResult<Charger> {
        if power_rating < MIN_POWER_RATING || power_rating > MAX_POWER_RATING {
            return Err(AppError::Validation(
                format!("Power rating must be between {} and {} kW", MIN_POWER_RATING, MAX_POWER_RATING).into(),
            ));
        }
        self.repo.update_power_rating(id, power_rating, updated_by).await
    }

    pub async fn hard_delete(&self, id: &str) -> AppResult<()> {
        let charger = self.repo.get_by_id(id).await?;
        match charger {
            Some(c) if c.is_active() => self.repo.hard_delete(id).await,
            Some(_) => Err(AppError::Validation("Charger is already deleted".into())),
            None => Err(AppError::NotFound(format!("Charger {} not found", id))),
        }
    }

    pub async fn soft_delete(&self, id: &str, updated_by: &str) -> AppResult<()> {
        let charger = self.repo.get_by_id(id).await?;
        match charger {
            Some(c) if c.is_active() => self.repo.soft_delete(id, updated_by).await,
            Some(_) => Err(AppError::Validation("Charger is already deleted".into())),
            None => Err(AppError::NotFound(format!("Charger {} not found", id))),
        }
    }

    pub async fn undelete(&self, id: &str, updated_by: &str) -> AppResult<Charger> {
        let charger = self.repo.get_by_id(id).await?;
        match charger {
            Some(c) if c.deleted_at.is_some() => self.repo.undelete(id, updated_by).await,
            Some(_) => Err(AppError::Validation("Charger is already active".into())),
            None => Err(AppError::NotFound(format!("Charger {} not found", id))),
        }
    }
}
