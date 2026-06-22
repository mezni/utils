use sqlx::postgres::PgPool;
use serde::Serialize;
use std::collections::HashMap;
use crate::domain::gis::{Station, Address};
use crate::ingestion::tag_normalizer::{TagNormalized, TagNormalizer};

/// ETL normalization service
pub struct EtlNormalizationService {
    pool: PgPool,
}

impl EtlNormalizationService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Normalize OSM tags to internal schema
    pub fn normalize(&self, tags: &HashMap<String, String>) -> NormalizedStation {
        let normalizer = TagNormalizer;
        let normalized = normalizer.normalize(tags);

        // Extract values
        let amenity = normalized.amenity;
        let power = normalized.power;
        let name = normalized.name;
        let operator = normalized.operator;
        let address = normalized.address;
        let connector_types = normalized.connector_types;

        NormalizedStation {
            amenity,
            power,
            name,
            operator,
            address,
            connector_types,
            tags: tags.clone(),
        }
    }

    /// Normalize tags and create station record
    pub async fn create_station_record(
        &self,
        normalized: NormalizedStation,
        latitude: f64,
        longitude: f64,
    ) -> Result<Station, sqlx::Error> {
        // Validate coordinates
        self.validate_coordinates(latitude, longitude)?;

        // Extract name if not provided
        let name = normalized.name;

        // Create station from normalized data
        let station = Station::from_db_row(
            self.generate_station_id(),
            name,
            latitude,
            longitude,
            amenity.clone(),
            power,
            Some(connector_types),
            true, // Default to available
            None, // last_updated will be set by trigger
            None, // created_at will be set by database
        );

        // Insert into database
        self.insert_station(&station).await?;

        Ok(station)
    }

    /// Normalize tags and create multiple station records
    pub async fn create_station_records(
        &self,
        station_data: &[NormalizedStation],
        latitudes: Vec<f64>,
        longitudes: Vec<f64>,
    ) -> Result<Vec<Station>, sqlx::Error> {
        if station_data.len() != latitudes.len() || station_data.len() != longitudes.len() {
            return Err(sqlx::Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Mismatched data arrays",
            )));
        }

        let mut stations = Vec::new();

        for (normalized, latitude, longitude) in station_data.iter().zip(latitudes.iter()).zip(longitudes.iter()) {
            let station = self.create_station_record(normalized.clone(), *latitude, *longitude).await?;
            stations.push(station);
        }

        Ok(stations)
    }

    /// Validate coordinates
    fn validate_coordinates(&self, latitude: f64, longitude: f64) -> Result<(), sqlx::Error> {
        if latitude < -90.0 || latitude > 90.0 {
            return Err(sqlx::Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid latitude: {}", latitude)
            )));
        }

        if longitude < -180.0 || longitude > 180.0 {
            return Err(sqlx::Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid longitude: {}", longitude)
            )));
        }

        Ok(())
    }

    /// Generate station ID using nanoid pattern
    fn generate_station_id(&self) -> String {
        format!("STA-{}", nanoid::nanoid!(12))
    }

    /// Insert station into database
    async fn insert_station(&self, station: &Station) -> Result<(), sqlx::Error> {
        let sql = r#"
            INSERT INTO gis.osm_charging_stations (
                id,
                osm_id,
                latitude,
                longitude,
                geom,
                station_name,
                operator,
                address,
                amenity,
                power,
                connector_types,
                is_available,
                last_updated,
                created_at
            ) VALUES ($1, $2, $3, $4, ST_SetSRID(ST_MakePoint($5, $6), 4326), $7, $8, $9, $10, $11, $12, $13, NOW(), NOW())
        "#;

        let address_json = serde_json::to_value(&station.address)
            .unwrap_or(serde_json::json!(null));

        sqlx::query(sql)
            .bind(&station.id)
            .bind(&station.id) // osm_id will be added later
            .bind(station.latitude)
            .bind(station.longitude)
            .bind(station.longitude)
            .bind(station.latitude)
            .bind(&station.name)
            .bind(&station.operator)
            .bind(&address_json)
            .bind(&station.amenity)
            .bind(&station.power)
            .bind(serde_json::to_value(&station.connector_types).unwrap_or_default())
            .bind(station.is_available)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

/// Normalized station data
#[derive(Debug, Clone, Serialize)]
pub struct NormalizedStation {
    pub amenity: String,
    pub power: Option<String>,
    pub name: String,
    pub operator: Option<String>,
    pub address: Option<Address>,
    pub connector_types: Vec<String>,
    pub tags: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_station_record_with_mock_pool() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect("postgresql://test:test@localhost:5432/test")
            .await
            .expect("Failed to connect to test database");

        let service = EtlNormalizationService::new(pool);

        let mut tags = HashMap::new();
        tags.insert("amenity".to_string(), "charging_station".to_string());
        tags.insert("power".to_string(), "50kW".to_string());
        tags.insert("name".to_string(), "Test Station".to_string());

        let normalized = service.normalize(&tags);

        let result = service.create_station_record(normalized, 40.7829, -73.9654).await;
        assert!(result.is_err()); // Expect error due to missing database
    }
}
