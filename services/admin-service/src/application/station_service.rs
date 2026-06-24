use std::sync::Arc;

use bornemap_platform_core::constants::{DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, MIN_PAGE_SIZE};
use bornemap_platform_core::error::AppResult;
use bornemap_platform_core::models::Station;
use bornemap_platform_db::traits::{PartnerRepository, StationRepository};

pub struct StationService {
    repo: Arc<dyn StationRepository>,
    partner_repo: Arc<dyn PartnerRepository>,
}

impl StationService {
    pub fn new(repo: Arc<dyn StationRepository>, partner_repo: Arc<dyn PartnerRepository>) -> Self {
        Self { repo, partner_repo }
    }

    pub async fn create(
        &self,
        name: &str,
        location: Option<&str>,
        partner_id: &str,
        created_by: &str,
    ) -> AppResult<Station> {
        let name = name.trim();
        if name.is_empty() {
            return Err(AppError::Validation("Name cannot be empty".into()));
        }
        if name.len() > 200 {
            return Err(AppError::Validation("Name cannot exceed 200 characters".into()));
        }
        let partner = self.partner_repo.get_by_id(partner_id).await?;
        match partner {
            Some(p) if p.is_active() => {}
            _ => return Err(AppError::NotFound(format!("Partner {} not found or inactive", partner_id))),
        }
        self.repo.create(name, location, partner_id, created_by, created_by).await
    }

    pub async fn get(&self, id: &str) -> AppResult<Station> {
        let station = self.repo.get_by_id(id).await?;
        match station {
            Some(s) if s.is_active() => Ok(s),
            _ => Err(AppError::NotFound(format!("Station {} not found", id))),
        }
    }

    pub async fn list(&self, page: u32, limit: u32, partner_id: Option<&str>) -> AppResult<(Vec<Station>, u64)> {
        let page = page.clamp(MIN_PAGE_SIZE, u32::MAX);
        let limit = limit.clamp(MIN_PAGE_SIZE, MAX_PAGE_SIZE);
        self.repo.list(page, limit, partner_id).await
    }

    pub async fn update(&self, id: &str, name: &str, location: Option<&str>, updated_by: &str) -> AppResult<Station> {
        let name = name.trim();
        if name.is_empty() {
            return Err(AppError::Validation("Name cannot be empty".into()));
        }
        let station = self.repo.get_by_id(id).await?;
        match station {
            Some(s) if s.is_active() => self.repo.update(id, name, location, updated_by).await,
            Some(_) => Err(AppError::Validation("Station is deleted".into())),
            None => Err(AppError::NotFound(format!("Station {} not found", id))),
        }
    }

    pub async fn hard_delete(&self, id: &str) -> AppResult<()> {
        let station = self.repo.get_by_id(id).await?;
        match station {
            Some(s) if s.is_active() => self.repo.hard_delete(id).await,
            Some(_) => Err(AppError::Validation("Station is already deleted".into())),
            None => Err(AppError::NotFound(format!("Station {} not found", id))),
        }
    }

    pub async fn soft_delete(&self, id: &str, updated_by: &str) -> AppResult<()> {
        let station = self.repo.get_by_id(id).await?;
        match station {
            Some(s) if s.is_active() => self.repo.soft_delete(id, updated_by).await,
            Some(_) => Err(AppError::Validation("Station is already deleted".into())),
            None => Err(AppError::NotFound(format!("Station {} not found", id))),
        }
    }

    pub async fn undelete(&self, id: &str, updated_by: &str) -> AppResult<Station> {
        let station = self.repo.get_by_id(id).await?;
        match station {
            Some(s) if s.deleted_at.is_some() => self.repo.undelete(id, updated_by).await,
            Some(_) => Err(AppError::Validation("Station is already active".into())),
            None => Err(AppError::NotFound(format!("Station {} not found", id))),
        }
    }
}
