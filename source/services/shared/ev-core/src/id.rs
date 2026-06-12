pub enum EntityPrefix {
    Station,
    Charger,
    Partner,
    User,
    Operator,
}

impl EntityPrefix {
    pub fn as_str(&self) -> &'static str {
        match self {
            EntityPrefix::Station => "STA",
            EntityPrefix::Charger => "CHR",
            EntityPrefix::Partner => "PRT",
            EntityPrefix::User => "USR",
            EntityPrefix::Operator => "OPR",
        }
    }
}

pub fn generate_entity_id(prefix: EntityPrefix) -> String {
    let nano = nanoid::nanoid!(21);
    format!("{}-{}", prefix.as_str(), nano)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_station_id_format() {
        let id = generate_entity_id(EntityPrefix::Station);
        assert!(id.starts_with("STA-"));
        assert_eq!(id.len(), 25); // "STA-" + 21 chars
    }

    #[test]
    fn test_charger_id_format() {
        let id = generate_entity_id(EntityPrefix::Charger);
        assert!(id.starts_with("CHR-"));
        assert_eq!(id.len(), 25);
    }

    #[test]
    fn test_partner_id_format() {
        let id = generate_entity_id(EntityPrefix::Partner);
        assert!(id.starts_with("PRT-"));
        assert_eq!(id.len(), 25);
    }

    #[test]
    fn test_ids_are_unique() {
        let ids: std::collections::HashSet<String> = (0..100)
            .map(|_| generate_entity_id(EntityPrefix::Station))
            .collect();
        assert_eq!(ids.len(), 100);
    }
}
