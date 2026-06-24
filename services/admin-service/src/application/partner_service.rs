use std::sync::Arc;

use bornemap_platform_core::constants::{DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, MIN_PAGE_SIZE};
use bornemap_platform_core::error::AppResult;
use bornemap_platform_core::models::Partner;
use bornemap_platform_db::traits::PartnerRepository;

pub struct PartnerService {
    repo: Arc<dyn PartnerRepository>,
}

impl PartnerService {
    pub fn new(repo: Arc<dyn PartnerRepository>) -> Self {
        Self { repo }
    }

    pub async fn create(&self, name: &str, created_by: &str) -> AppResult<Partner> {
        let name = name.trim();
        if name.is_empty() {
            return Err(AppError::Validation("Name cannot be empty".into()));
        }
        if name.len() > 200 {
            return Err(AppError::Validation("Name cannot exceed 200 characters".into()));
        }
        if !name.chars().all(|c| c.is_alphanumeric() || c == ' ' || c == '-') {
            return Err(AppError::Validation(
                "Name can only contain letters, numbers, spaces, and hyphens".into(),
            ));
        }
        self.repo.create(name, created_by, created_by).await
    }

    pub async fn get(&self, id: &str) -> AppResult<Partner> {
        let partner = self.repo.get_by_id(id).await?;
        match partner {
            Some(p) if p.is_active() => Ok(p),
            _ => Err(AppError::NotFound(format!("Partner {} not found", id))),
        }
    }

    pub async fn list(&self, page: u32, limit: u32) -> AppResult<(Vec<Partner>, u64)> {
        let page = page.clamp(MIN_PAGE_SIZE, u32::MAX);
        let limit = limit.clamp(MIN_PAGE_SIZE, MAX_PAGE_SIZE);
        self.repo.list(page, limit).await
    }

    pub async fn update(&self, id: &str, name: &str, updated_by: &str) -> AppResult<Partner> {
        let name = name.trim();
        if name.is_empty() {
            return Err(AppError::Validation("Name cannot be empty".into()));
        }
        if name.len() > 200 {
            return Err(AppError::Validation("Name cannot exceed 200 characters".into()));
        }
        if !name.chars().all(|c| c.is_alphanumeric() || c == ' ' || c == '-') {
            return Err(AppError::Validation(
                "Name can only contain letters, numbers, spaces, and hyphens".into(),
            ));
        }
        let partner = self.repo.get_by_id(id).await?;
        match partner {
            Some(p) if p.is_active() => self.repo.update(id, name, updated_by).await,
            Some(_) => Err(AppError::Validation("Partner is deleted".into())),
            None => Err(AppError::NotFound(format!("Partner {} not found", id))),
        }
    }

    pub async fn hard_delete(&self, id: &str) -> AppResult<()> {
        let partner = self.repo.get_by_id(id).await?;
        match partner {
            Some(p) if p.is_active() => self.repo.hard_delete(id).await,
            Some(_) => Err(AppError::Validation("Partner is already deleted".into())),
            None => Err(AppError::NotFound(format!("Partner {} not found", id))),
        }
    }

    pub async fn soft_delete(&self, id: &str, updated_by: &str) -> AppResult<()> {
        let partner = self.repo.get_by_id(id).await?;
        match partner {
            Some(p) if p.is_active() => self.repo.soft_delete(id, updated_by).await,
            Some(_) => Err(AppError::Validation("Partner is already deleted".into())),
            None => Err(AppError::NotFound(format!("Partner {} not found", id))),
        }
    }

    pub async fn undelete(&self, id: &str, updated_by: &str) -> AppResult<Partner> {
        let partner = self.repo.get_by_id(id).await?;
        match partner {
            Some(p) if p.deleted_at.is_some() => self.repo.undelete(id, updated_by).await,
            Some(_) => Err(AppError::Validation("Partner is already active".into())),
            None => Err(AppError::NotFound(format!("Partner {} not found", id))),
        }
    }
}
