use crate::domain::station::{Station, CreateStationRequest, UpdateStationRequest, validate_lat, validate_lon};
use crate::domain::errors::ServiceError;
use crate::infrastructure::repository::StationRepository;

pub struct StationUseCases {
    repo: StationRepository,
}

impl StationUseCases {
    pub fn new(repo: StationRepository) -> Self {
        Self { repo }
    }

    pub async fn create(&self, req: CreateStationRequest) -> Result<Station, ServiceError> {
        let name = req.name.trim().to_string();
        if name.is_empty() {
            return Err(ServiceError::Validation("name is required".into()));
        }
        if !validate_lat(req.lat) {
            return Err(ServiceError::Validation("lat must be between -90 and 90".into()));
        }
        if !validate_lon(req.lon) {
            return Err(ServiceError::Validation("lon must be between -180 and 180".into()));
        }
        let mut station = Station::new(name, req.lat, req.lon);
        station.osm_id = req.osm_id;
        station.partner_id = req.partner_id;
        station.address = req.address;
        self.repo.insert(&station).await?;
        Ok(station)
    }

    pub async fn get(&self, station_id: &str) -> Result<Station, ServiceError> {
        self.repo.find_by_id(station_id).await
    }

    pub async fn list(
        &self,
        page: i64,
        per_page: i64,
        partner_id: Option<&str>,
    ) -> Result<(Vec<Station>, i64), ServiceError> {
        let offset = (page - 1) * per_page;
        let stations = self.repo.list(per_page, offset, partner_id).await?;
        let total = self.repo.count(partner_id).await?;
        Ok((stations, total))
    }

    pub async fn update(&self, station_id: &str, req: UpdateStationRequest) -> Result<Station, ServiceError> {
        if let Some(lat) = req.lat {
            if !validate_lat(lat) {
                return Err(ServiceError::Validation("lat must be between -90 and 90".into()));
            }
        }
        if let Some(lon) = req.lon {
            if !validate_lon(lon) {
                return Err(ServiceError::Validation("lon must be between -180 and 180".into()));
            }
        }
        let existing = self.repo.find_by_id(station_id).await?;
        let name = req.name.unwrap_or(existing.name);
        let address = req.address.or(existing.address);
        let lat = req.lat.unwrap_or(existing.lat);
        let lon = req.lon.unwrap_or(existing.lon);
        let partner_id = req.partner_id.or(existing.partner_id);
        self.repo.update(station_id, &name, address.as_deref(), lat, lon, partner_id.as_deref()).await
    }

    pub async fn delete(&self, station_id: &str) -> Result<(), ServiceError> {
        self.repo.soft_delete(station_id).await
    }
}
