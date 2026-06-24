use crate::domain::station::Station;
use crate::domain::errors::NearbyError;
use crate::infrastructure::repository::PgStationRepository;

pub struct NearbyQuery {
    pub lat: f64,
    pub lon: f64,
    pub radius: Option<i32>,
    pub limit: Option<i32>,
}

pub struct GetNearbyStationsUseCase {
    repository: PgStationRepository,
}

impl GetNearbyStationsUseCase {
    pub fn new(repository: PgStationRepository) -> Self {
        Self { repository }
    }

    pub async fn execute(&self, query: NearbyQuery) -> Result<Vec<Station>, NearbyError> {
        let radius = query.radius.unwrap_or(5000);
        let limit = query.limit.unwrap_or(50);

        if radius <= 0 {
            return Err(NearbyError::Validation("radius must be positive".into()));
        }
        if !(1..=100).contains(&limit) {
            return Err(NearbyError::Validation("limit must be between 1 and 100".into()));
        }

        self.repository.find_nearby(query.lat, query.lon, radius, limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_radius() {
        let q = NearbyQuery { lat: 36.8, lon: 10.1, radius: None, limit: None };
        assert_eq!(q.radius.unwrap_or(5000), 5000);
    }

    #[test]
    fn test_default_limit() {
        let q = NearbyQuery { lat: 36.8, lon: 10.1, radius: None, limit: None };
        assert_eq!(q.limit.unwrap_or(50), 50);
    }

    #[test]
    fn test_custom_radius() {
        let q = NearbyQuery { lat: 36.8, lon: 10.1, radius: Some(1000), limit: None };
        assert_eq!(q.radius.unwrap_or(5000), 1000);
    }

    #[test]
    fn test_custom_limit() {
        let q = NearbyQuery { lat: 36.8, lon: 10.1, radius: None, limit: Some(10) };
        assert_eq!(q.limit.unwrap_or(50), 10);
    }
}
