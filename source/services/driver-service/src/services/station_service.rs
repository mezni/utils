use crate::error::DomainError;
use crate::models::Station;
use crate::repositories::StationRepository;

pub struct StationService<R: StationRepository> {
    repo: R,
}

impl<R: StationRepository> StationService<R> {
    pub fn new(repo: R) -> Self {
        Self { repo }
    }

    pub async fn list_stations(&self) -> Result<Vec<Station>, DomainError> {
        self.repo.find_all().await
    }

    pub async fn get_station(&self, id: &str) -> Result<Station, DomainError> {
        self.repo
            .find_by_id(id)
            .await?
            .ok_or_else(|| DomainError::NotFound(format!("Station with id '{}' not found", id)))
    }

    pub async fn find_nearby(
        &self,
        lat: f64,
        lng: f64,
        radius: f64,
    ) -> Result<Vec<Station>, DomainError> {
        if !(-90.0..=90.0).contains(&lat) {
            return Err(DomainError::BadRequest(format!(
                "lat must be between -90 and 90, got {}",
                lat
            )));
        }
        if !(-180.0..=180.0).contains(&lng) {
            return Err(DomainError::BadRequest(format!(
                "lng must be between -180 and 180, got {}",
                lng
            )));
        }
        if radius <= 0.0 {
            return Err(DomainError::BadRequest(format!(
                "radius must be greater than 0, got {}",
                radius
            )));
        }

        self.repo.find_nearby(lat, lng, radius).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repositories::MockStationRepository;

    fn make_station(id: &str) -> Station {
        Station {
            id: id.to_string(),
            name: format!("Station {}", id),
            status: "active".to_string(),
            latitude: Some(36.8),
            longitude: Some(10.2),
            distance: 0.0,
        }
    }

    #[tokio::test]
    async fn test_list_stations() {
        let mut mock = MockStationRepository::new();

        mock.expect_find_all().times(1).returning(|| {
            Ok(vec![
                make_station("STA-00001"),
                make_station("STA-00002"),
            ])
        });

        let service = StationService::new(mock);
        let result = service.list_stations().await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_get_station_found() {
        let mut mock = MockStationRepository::new();

        mock.expect_find_by_id()
            .with(mockall::predicate::eq("STA-00001"))
            .times(1)
            .returning(|_| Ok(Some(make_station("STA-00001"))));

        let service = StationService::new(mock);
        let result = service.get_station("STA-00001").await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().id, "STA-00001");
    }

    #[tokio::test]
    async fn test_get_station_not_found() {
        let mut mock = MockStationRepository::new();

        mock.expect_find_by_id()
            .with(mockall::predicate::eq("NONEXISTENT"))
            .times(1)
            .returning(|_| Ok(None));

        let service = StationService::new(mock);
        let result = service.get_station("NONEXISTENT").await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DomainError::NotFound(_)));
    }

    #[tokio::test]
    async fn test_find_nearby_valid_params() {
        let mut mock = MockStationRepository::new();

        mock.expect_find_nearby()
            .with(
                mockall::predicate::eq(36.8),
                mockall::predicate::eq(10.2),
                mockall::predicate::eq(5000.0),
            )
            .times(1)
            .returning(|_, _, _| Ok(vec![make_station("STA-00001")]));

        let service = StationService::new(mock);
        let result = service.find_nearby(36.8, 10.2, 5000.0).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_find_nearby_invalid_lat() {
        let mock = MockStationRepository::new();
        let service = StationService::new(mock);

        let result = service.find_nearby(100.0, 10.0, 5000.0).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DomainError::BadRequest(_)));
    }

    #[tokio::test]
    async fn test_find_nearby_invalid_radius() {
        let mock = MockStationRepository::new();
        let service = StationService::new(mock);

        let result = service.find_nearby(36.8, 10.2, 0.0).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DomainError::BadRequest(_)));
    }
}
