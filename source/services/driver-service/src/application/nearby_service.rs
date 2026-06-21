use crate::domain::nearby_query::{NearbyQuery, ValidationError};
use crate::domain::nearby_result::NearbyResult;
use crate::infrastructure::NearbyRepository;

pub struct NearbyService {
    repo: NearbyRepository,
}

impl NearbyService {
    pub fn new(repo: NearbyRepository) -> Self {
        Self { repo }
    }

    pub async fn find_nearby(
        &self,
        lat: f64,
        lng: f64,
        radius_meters: f64,
    ) -> Result<Vec<NearbyResult>, NearbyServiceError> {
        let query = NearbyQuery::new(lat, lng, radius_meters)?;
        let results = self.repo.find_nearby(&query).await?;
        Ok(results)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NearbyServiceError {
    #[error("validation error: {0}")]
    Validation(#[from] ValidationError),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}
