use crate::domain::entities::station::Station;
use crate::domain::repositories::partner_repo::PartnerRepository;
use crate::domain::repositories::station_repo::StationRepository;
use crate::domain::value_objects::geo::Geo;
use crate::domain::value_objects::ids;

pub struct CreateStationInput {
    pub partner_id: String,
    pub name: String,
    pub address: String,
    pub latitude: f64,
    pub longitude: f64,
}

pub struct CreateStationUseCase<R: StationRepository, P: PartnerRepository> {
    station_repo: R,
    partner_repo: P,
}

impl<R: StationRepository, P: PartnerRepository> CreateStationUseCase<R, P> {
    pub fn new(station_repo: R, partner_repo: P) -> Self {
        Self {
            station_repo,
            partner_repo,
        }
    }

    pub async fn execute(&self, input: CreateStationInput) -> Result<Station, String> {
        Geo::new(input.latitude, input.longitude)?;

        let name = input.name.trim().to_string();
        if name.is_empty() {
            return Err("Station name cannot be empty".to_string());
        }
        if name.len() > 150 {
            return Err("Station name must be 150 characters or less".to_string());
        }

        let address = input.address.trim().to_string();
        if address.is_empty() {
            return Err("Station address cannot be empty".to_string());
        }
        if address.len() > 250 {
            return Err("Station address must be 250 characters or less".to_string());
        }

        let partner = self
            .partner_repo
            .find_by_id(&input.partner_id)
            .await?
            .ok_or_else(|| format!("Partner {} not found", input.partner_id))?;

        let station = Station {
            id: ids::generate_station_id(),
            partner_id: partner.id,
            name,
            address,
            latitude: input.latitude,
            longitude: input.longitude,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        self.station_repo.create(&station).await
    }
}
