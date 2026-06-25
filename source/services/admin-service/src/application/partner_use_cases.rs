use crate::domain::partner::{Partner, CreatePartnerRequest, UpdatePartnerRequest, validate_partner_type};
use crate::domain::errors::ServiceError;
use crate::infrastructure::repository::PartnerRepository;

pub struct PartnerUseCases {
    repo: PartnerRepository,
}

impl PartnerUseCases {
    pub fn new(repo: PartnerRepository) -> Self {
        Self { repo }
    }

    pub async fn create(&self, req: CreatePartnerRequest) -> Result<Partner, ServiceError> {
        let name = req.name.trim().to_string();
        if name.is_empty() {
            return Err(ServiceError::Validation("name is required".into()));
        }
        if let Some(ref pt) = req.partner_type {
            if !validate_partner_type(pt) {
                return Err(ServiceError::Validation(
                    "partner_type must be INDIVIDUAL or COMPANY".into(),
                ));
            }
        }
        let mut partner = Partner::new(name, req.partner_type);
        partner.support_phone = req.support_phone;
        partner.support_email = req.support_email;
        self.repo.insert(&partner).await?;
        Ok(partner)
    }

    pub async fn get(&self, partner_id: &str) -> Result<Partner, ServiceError> {
        self.repo.find_by_id(partner_id).await
    }

    pub async fn list(
        &self,
        page: i64,
        per_page: i64,
        search: Option<&str>,
    ) -> Result<(Vec<Partner>, i64), ServiceError> {
        let offset = (page - 1) * per_page;
        let partners = self.repo.list(per_page, offset, search).await?;
        let total = self.repo.count(search).await?;
        Ok((partners, total))
    }

    pub async fn update(&self, partner_id: &str, req: UpdatePartnerRequest) -> Result<Partner, ServiceError> {
        if let Some(ref pt) = req.partner_type {
            if !validate_partner_type(pt) {
                return Err(ServiceError::Validation(
                    "partner_type must be INDIVIDUAL or COMPANY".into(),
                ));
            }
        }
        let existing = self.repo.find_by_id(partner_id).await?;
        let name = req.name.unwrap_or(existing.name);
        let partner_type = req.partner_type.or(existing.partner_type);
        let support_phone = req.support_phone.or(existing.support_phone);
        let support_email = req.support_email.or(existing.support_email);
        self.repo.update(partner_id, &name, partner_type.as_deref(), support_phone.as_deref(), support_email.as_deref()).await
    }

    pub async fn delete(&self, partner_id: &str) -> Result<(), ServiceError> {
        self.repo.soft_delete(partner_id).await
    }
}
