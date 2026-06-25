use crate::domain::charger::{Charger, CreateChargerRequest, UpdateChargerRequest, validate_charger_counts};
use crate::domain::errors::ServiceError;
use crate::infrastructure::repository::ChargerRepository;

pub struct ChargerUseCases {
    repo: ChargerRepository,
}

impl ChargerUseCases {
    pub fn new(repo: ChargerRepository) -> Self {
        Self { repo }
    }

    pub async fn create(&self, req: CreateChargerRequest) -> Result<Charger, ServiceError> {
        let available = req.count_available.unwrap_or(1);
        let total = req.count_total.unwrap_or(1);
        if !validate_charger_counts(available, total) {
            return Err(ServiceError::Validation(
                "count_available >= 0, count_total >= 1, count_total >= count_available".into(),
            ));
        }
        let mut charger = Charger::new(req.station_id, req.connector_type_id, req.status_id, req.current_type_id);
        charger.power_kw = req.power_kw;
        charger.voltage = req.voltage;
        charger.amperage = req.amperage;
        charger.count_available = available;
        charger.count_total = total;
        self.repo.insert(&charger).await?;
        Ok(charger)
    }

    pub async fn get(&self, charger_id: &str) -> Result<Charger, ServiceError> {
        self.repo.find_by_id(charger_id).await
    }

    pub async fn list(
        &self,
        page: i64,
        per_page: i64,
        station_id: Option<&str>,
    ) -> Result<(Vec<Charger>, i64), ServiceError> {
        let offset = (page - 1) * per_page;
        let chargers = self.repo.list(per_page, offset, station_id).await?;
        let total = self.repo.count(station_id).await?;
        Ok((chargers, total))
    }

    pub async fn update(&self, charger_id: &str, req: UpdateChargerRequest) -> Result<Charger, ServiceError> {
        let existing = self.repo.find_by_id(charger_id).await?;
        let available = req.count_available.unwrap_or(existing.count_available);
        let total = req.count_total.unwrap_or(existing.count_total);
        if !validate_charger_counts(available, total) {
            return Err(ServiceError::Validation(
                "count_available >= 0, count_total >= 1, count_total >= count_available".into(),
            ));
        }
        self.repo.update(
            charger_id,
            req.connector_type_id.unwrap_or(existing.connector_type_id),
            req.status_id.unwrap_or(existing.status_id),
            req.current_type_id.unwrap_or(existing.current_type_id),
            req.power_kw.or(existing.power_kw),
            req.voltage.or(existing.voltage),
            req.amperage.or(existing.amperage),
            available,
            total,
        ).await
    }

    pub async fn delete(&self, charger_id: &str) -> Result<(), ServiceError> {
        self.repo.soft_delete(charger_id).await
    }
}
