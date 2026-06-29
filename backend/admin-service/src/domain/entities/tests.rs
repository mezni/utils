#[cfg(test)]
mod tests {
    use crate::domain::entities::partner::Partner;
    use crate::domain::entities::station::Station;
    use crate::domain::entities::connector::Connector;
    use crate::domain::value_objects::ids;
    use crate::domain::value_objects::geo::Geo;
    use chrono::{DateTime, Utc};

    #[test]
    fn test_partner_creation() {
        let id = ids::generate_partner_id();
        let name = "Test Partner".to_string();
        let created_at = Utc::now();
        let updated_at = Utc::now();

        let partner = Partner {
            id: id.clone(),
            name: name.clone(),
            created_at,
            updated_at,
        };

        assert_eq!(partner.id, id);
        assert_eq!(partner.name, name);
        assert_eq!(partner.id.starts_with("PRT_"), true);
        assert_eq!(partner.id.len(), 13); // PRT_ + 8 chars
    }

    #[test]
    fn test_station_creation() {
        let id = ids::generate_station_id();
        let partner_id = ids::generate_partner_id();
        let name = "Test Station".to_string();
        let address = "123 Main St".to_string();
        let latitude = 36.8065;
        let longitude = 10.1815;
        let created_at = Utc::now();
        let updated_at = Utc::now();

        let station = Station {
            id: id.clone(),
            partner_id: partner_id.clone(),
            name: name.clone(),
            address: address.clone(),
            latitude,
            longitude,
            created_at,
            updated_at,
        };

        assert_eq!(station.id, id);
        assert_eq!(station.partner_id, partner_id);
        assert_eq!(station.name, name);
        assert_eq!(station.address, address);
        assert_eq!(station.latitude, latitude);
        assert_eq!(station.longitude, longitude);
        assert_eq!(station.id.starts_with("STN_"), true);
        assert_eq!(station.id.len(), 13); // STN_ + 8 chars
    }

    #[test]
    fn test_connector_creation() {
        let id = ids::generate_connector_id();
        let station_id = ids::generate_station_id();
        let connector_type = "CCS2".to_string();
        let power_kw = 150.0;
        let created_at = Utc::now();
        let updated_at = Utc::now();

        let connector = Connector {
            id: id.clone(),
            station_id: station_id.clone(),
            connector_type: connector_type.clone(),
            power_kw,
            created_at,
            updated_at,
        };

        assert_eq!(connector.id, id);
        assert_eq!(connector.station_id, station_id);
        assert_eq!(connector.connector_type, connector_type);
        assert_eq!(connector.power_kw, power_kw);
        assert_eq!(connector.id.starts_with("CON_"), true);
        assert_eq!(connector.id.len(), 13); // CON_ + 8 chars
    }

    #[test]
    fn test_geo_validation() {
        // Valid coordinates
        let geo = Geo::new(36.8065, 10.1815);
        assert!(geo.is_ok());

        // Invalid latitude
        let geo = Geo::new(100.0, 10.1815);
        assert!(geo.is_err());

        // Invalid longitude
        let geo = Geo::new(36.8065, 200.0);
        assert!(geo.is_err());

        // Boundary latitudes
        let geo = Geo::new(90.0, 0.0);
        assert!(geo.is_ok());
        let geo = Geo::new(-90.0, 0.0);
        assert!(geo.is_ok());

        // Boundary longitudes
        let geo = Geo::new(0.0, 180.0);
        assert!(geo.is_ok());
        let geo = Geo::new(0.0, -180.0);
        assert!(geo.is_ok());
    }

    #[test]
    fn test_id_uniqueness() {
        let id1 = ids::generate_partner_id();
        let id2 = ids::generate_partner_id();
        assert_ne!(id1, id2);

        let id3 = ids::generate_station_id();
        let id4 = ids::generate_station_id();
        assert_ne!(id3, id4);

        let id5 = ids::generate_connector_id();
        let id6 = ids::generate_connector_id();
        assert_ne!(id5, id6);
    }

    #[test]
    fn test_id_format() {
        let partner_id = ids::generate_partner_id();
        assert!(partner_id.starts_with("PRT_"));
        assert_eq!(partner_id.len(), 13);

        let station_id = ids::generate_station_id();
        assert!(station_id.starts_with("STN_"));
        assert_eq!(station_id.len(), 13);

        let connector_id = ids::generate_connector_id();
        assert!(connector_id.starts_with("CON_"));
        assert_eq!(connector_id.len(), 13);
    }
}