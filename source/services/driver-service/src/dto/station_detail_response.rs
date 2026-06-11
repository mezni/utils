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

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_charger() -> borne_data::Charger {
        borne_data::Charger {
            id: "ch_1".into(),
            station_id: "st_1".into(),
            connector_type: "CCS".into(),
            power_kw: 150.0,
            status: "available".into(),
            created_at: borne_data::chrono::Utc::now(),
            created_by: None,
            updated_at: borne_data::chrono::Utc::now(),
            updated_by: None,
        }
    }

    fn sample_partner() -> borne_data::Partner {
        borne_data::Partner {
            id: "p_1".into(),
            name: "Partner Co".into(),
            r#type: "operator".into(),
            is_verified: true,
            is_active: true,
            is_live: true,
            created_at: borne_data::chrono::Utc::now(),
            created_by: None,
            updated_at: borne_data::chrono::Utc::now(),
            updated_by: None,
        }
    }

    #[test]
    fn charger_from_converts() {
        let c: ChargerResponse = sample_charger().into();
        assert_eq!(c.id, "ch_1");
        assert_eq!(c.connector_type, "CCS");
        assert_eq!(c.power_kw, 150.0);
        assert_eq!(c.status, "available");
    }

    #[test]
    fn charger_serializes() {
        let c = ChargerResponse {
            id: "ch_2".into(),
            connector_type: "Type2".into(),
            power_kw: 22.0,
            status: "charging".into(),
        };
        let json = serde_json::to_value(&c).unwrap();
        assert_eq!(json["id"], "ch_2");
        assert_eq!(json["connector_type"], "Type2");
        assert_eq!(json["power_kw"], 22.0);
        assert_eq!(json["status"], "charging");
    }

    #[test]
    fn partner_from_converts() {
        let p: PartnerResponse = sample_partner().into();
        assert_eq!(p.id, "p_1");
        assert_eq!(p.name, "Partner Co");
        assert_eq!(p.r#type, "operator");
    }

    #[test]
    fn partner_serializes() {
        let p = PartnerResponse {
            id: "p_2".into(),
            name: "Test Partner".into(),
            r#type: "owner".into(),
        };
        let json = serde_json::to_value(&p).unwrap();
        assert_eq!(json["id"], "p_2");
        assert_eq!(json["name"], "Test Partner");
        assert_eq!(json["type"], "owner");
    }

    #[test]
    fn detail_response_serializes() {
        let resp = StationDetailResponse {
            id: "st_1".into(),
            name: "Main Station".into(),
            address: Some("Addr".into()),
            latitude: 36.8,
            longitude: 10.18,
            chargers: vec![ChargerResponse {
                id: "ch_1".into(),
                connector_type: "CCS".into(),
                power_kw: 150.0,
                status: "available".into(),
            }],
            partner: PartnerResponse {
                id: "p_1".into(),
                name: "Partner Co".into(),
                r#type: "operator".into(),
            },
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["id"], "st_1");
        assert!(json["chargers"].is_array());
        assert_eq!(json["chargers"][0]["connector_type"], "CCS");
        assert_eq!(json["partner"]["name"], "Partner Co");
    }
}
