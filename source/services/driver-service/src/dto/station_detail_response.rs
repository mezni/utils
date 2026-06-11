use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ChargerResponse {
    pub id: String,
    pub connector_type: String,
    pub power_kw: f64,
    pub status: String,
}

impl From<borne_data::Charger> for ChargerResponse {
    fn from(c: borne_data::Charger) -> Self {
        ChargerResponse {
            id: c.id,
            connector_type: c.connector_type,
            power_kw: c.power_kw,
            status: c.status,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct PartnerResponse {
    pub id: String,
    pub name: String,
    pub r#type: String,
}

impl From<borne_data::Partner> for PartnerResponse {
    fn from(p: borne_data::Partner) -> Self {
        PartnerResponse {
            id: p.id,
            name: p.name,
            r#type: p.r#type,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct StationDetailResponse {
    pub id: String,
    pub name: String,
    pub address: Option<String>,
    pub latitude: f64,
    pub longitude: f64,
    pub chargers: Vec<ChargerResponse>,
    pub partner: PartnerResponse,
}
