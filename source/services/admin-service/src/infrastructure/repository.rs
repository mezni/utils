use sqlx::PgPool;
use crate::domain::partner::Partner;
use crate::domain::station::Station;
use crate::domain::charger::Charger;
use crate::domain::errors::ServiceError;

pub struct PartnerRepository {
    pool: PgPool,
}

impl PartnerRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn insert(&self, partner: &Partner) -> Result<(), ServiceError> {
        sqlx::query(
            r#"INSERT INTO ev.partners (partner_id, name, partner_type, support_phone, support_email)
               VALUES ($1, $2, $3, $4, $5)"#,
        )
        .bind(&partner.partner_id)
        .bind(&partner.name)
        .bind(&partner.partner_type)
        .bind(&partner.support_phone)
        .bind(&partner.support_email)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn find_by_id(&self, partner_id: &str) -> Result<Partner, ServiceError> {
        let row = sqlx::query_as::<_, PartnerRow>(
            r#"SELECT partner_id, name, partner_type, support_phone, support_email,
                      is_verified, created_by_uuid, updated_by_uuid,
                      created_at, updated_at, deleted_at
               FROM ev.partners
               WHERE partner_id = $1 AND deleted_at IS NULL"#,
        )
        .bind(partner_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|r| r.into())
            .ok_or_else(|| ServiceError::NotFound(format!("partner {}", partner_id)))
    }

    pub async fn list(&self, limit: i64, offset: i64, search: Option<&str>) -> Result<Vec<Partner>, ServiceError> {
        let rows = sqlx::query_as::<_, PartnerRow>(
            r#"SELECT partner_id, name, partner_type, support_phone, support_email,
                      is_verified, created_by_uuid, updated_by_uuid,
                      created_at, updated_at, deleted_at
               FROM ev.partners
               WHERE deleted_at IS NULL
                 AND ($1::text IS NULL OR name ILIKE '%' || $1 || '%')
               ORDER BY created_at DESC
               LIMIT $2 OFFSET $3"#,
        )
        .bind(search)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.into()).collect())
    }

    pub async fn count(&self, search: Option<&str>) -> Result<i64, ServiceError> {
        let row = sqlx::query_scalar::<_, i64>(
            r#"SELECT COUNT(*) FROM ev.partners
               WHERE deleted_at IS NULL
                 AND ($1::text IS NULL OR name ILIKE '%' || $1 || '%')"#,
        )
        .bind(search)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn update(
        &self,
        partner_id: &str,
        name: &str,
        partner_type: Option<&str>,
        support_phone: Option<&str>,
        support_email: Option<&str>,
    ) -> Result<Partner, ServiceError> {
        sqlx::query(
            r#"UPDATE ev.partners
               SET name = $1, partner_type = $2, support_phone = $3,
                   support_email = $4, updated_at = NOW()
               WHERE partner_id = $5 AND deleted_at IS NULL"#,
        )
        .bind(name)
        .bind(partner_type)
        .bind(support_phone)
        .bind(support_email)
        .bind(partner_id)
        .execute(&self.pool)
        .await?;

        self.find_by_id(partner_id).await
    }

    pub async fn soft_delete(&self, partner_id: &str) -> Result<(), ServiceError> {
        let affected = sqlx::query(
            r#"UPDATE ev.partners SET deleted_at = NOW()
               WHERE partner_id = $1 AND deleted_at IS NULL"#,
        )
        .bind(partner_id)
        .execute(&self.pool)
        .await?
        .rows_affected();

        if affected == 0 {
            return Err(ServiceError::NotFound(format!("partner {}", partner_id)));
        }
        Ok(())
    }
}

#[derive(sqlx::FromRow)]
struct PartnerRow {
    partner_id: String,
    name: String,
    partner_type: Option<String>,
    support_phone: Option<String>,
    support_email: Option<String>,
    is_verified: bool,
    created_by_uuid: Option<uuid::Uuid>,
    updated_by_uuid: Option<uuid::Uuid>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: Option<chrono::DateTime<chrono::Utc>>,
    deleted_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<PartnerRow> for Partner {
    fn from(r: PartnerRow) -> Self {
        Self {
            partner_id: r.partner_id,
            name: r.name,
            partner_type: r.partner_type,
            support_phone: r.support_phone,
            support_email: r.support_email,
            is_verified: r.is_verified,
            created_by_uuid: r.created_by_uuid,
            updated_by_uuid: r.updated_by_uuid,
            created_at: r.created_at,
            updated_at: r.updated_at,
            deleted_at: r.deleted_at,
        }
    }
}

pub struct StationRepository {
    pool: PgPool,
}

impl StationRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn insert(&self, station: &Station) -> Result<(), ServiceError> {
        sqlx::query(
            r#"INSERT INTO ev.stations (station_id, osm_id, partner_id, name, address, location, tags)
               VALUES ($1, $2, $3, $4, $5,
                       ST_SetSRID(ST_MakePoint($6, $7), 4326)::geography,
                       $8::hstore)"#,
        )
        .bind(&station.station_id)
        .bind(station.osm_id)
        .bind(&station.partner_id)
        .bind(&station.name)
        .bind(&station.address)
        .bind(station.lon)
        .bind(station.lat)
        .bind(Option::<String>::None) // tags
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn find_by_id(&self, station_id: &str) -> Result<Station, ServiceError> {
        let row = sqlx::query_as::<_, StationRow>(
            r#"SELECT station_id, osm_id, partner_id, name, address,
                      ST_X(location::geometry) AS lon,
                      ST_Y(location::geometry) AS lat,
                      created_by_uuid, updated_by_uuid,
                      created_at, updated_at, deleted_at
               FROM ev.stations
               WHERE station_id = $1 AND deleted_at IS NULL"#,
        )
        .bind(station_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|r| r.into())
            .ok_or_else(|| ServiceError::NotFound(format!("station {}", station_id)))
    }

    pub async fn list(&self, limit: i64, offset: i64, partner_id: Option<&str>) -> Result<Vec<Station>, ServiceError> {
        let rows = sqlx::query_as::<_, StationRow>(
            r#"SELECT station_id, osm_id, partner_id, name, address,
                      ST_X(location::geometry) AS lon,
                      ST_Y(location::geometry) AS lat,
                      created_by_uuid, updated_by_uuid,
                      created_at, updated_at, deleted_at
               FROM ev.stations
               WHERE deleted_at IS NULL
                 AND ($1::text IS NULL OR partner_id = $1)
               ORDER BY created_at DESC
               LIMIT $2 OFFSET $3"#,
        )
        .bind(partner_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.into()).collect())
    }

    pub async fn count(&self, partner_id: Option<&str>) -> Result<i64, ServiceError> {
        let row = sqlx::query_scalar::<_, i64>(
            r#"SELECT COUNT(*) FROM ev.stations
               WHERE deleted_at IS NULL
                 AND ($1::text IS NULL OR partner_id = $1)"#,
        )
        .bind(partner_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn update(
        &self,
        station_id: &str,
        name: &str,
        address: Option<&str>,
        lat: f64,
        lon: f64,
        partner_id: Option<&str>,
    ) -> Result<Station, ServiceError> {
        sqlx::query(
            r#"UPDATE ev.stations
               SET name = $1, address = $2,
                   location = ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography,
                   partner_id = $5, updated_at = NOW()
               WHERE station_id = $6 AND deleted_at IS NULL"#,
        )
        .bind(name)
        .bind(address)
        .bind(lon)
        .bind(lat)
        .bind(partner_id)
        .bind(station_id)
        .execute(&self.pool)
        .await?;

        self.find_by_id(station_id).await
    }

    pub async fn soft_delete(&self, station_id: &str) -> Result<(), ServiceError> {
        let affected = sqlx::query(
            r#"UPDATE ev.stations SET deleted_at = NOW()
               WHERE station_id = $1 AND deleted_at IS NULL"#,
        )
        .bind(station_id)
        .execute(&self.pool)
        .await?
        .rows_affected();

        if affected == 0 {
            return Err(ServiceError::NotFound(format!("station {}", station_id)));
        }
        Ok(())
    }
}

#[derive(sqlx::FromRow)]
struct StationRow {
    station_id: String,
    osm_id: Option<i64>,
    partner_id: Option<String>,
    name: String,
    address: Option<String>,
    lat: f64,
    lon: f64,
    created_by_uuid: Option<uuid::Uuid>,
    updated_by_uuid: Option<uuid::Uuid>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: Option<chrono::DateTime<chrono::Utc>>,
    deleted_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<StationRow> for Station {
    fn from(r: StationRow) -> Self {
        Self {
            station_id: r.station_id,
            osm_id: r.osm_id,
            partner_id: r.partner_id,
            name: r.name,
            address: r.address,
            lat: r.lat,
            lon: r.lon,
            created_by_uuid: r.created_by_uuid,
            updated_by_uuid: r.updated_by_uuid,
            created_at: r.created_at,
            updated_at: r.updated_at,
            deleted_at: r.deleted_at,
        }
    }
}

pub struct ChargerRepository {
    pool: PgPool,
}

impl ChargerRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn insert(&self, charger: &Charger) -> Result<(), ServiceError> {
        sqlx::query(
            r#"INSERT INTO ev.chargers
                  (charger_id, station_id, connector_type_id, status_id, current_type_id,
                   power_kw, voltage, amperage, count_available, count_total)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"#,
        )
        .bind(&charger.charger_id)
        .bind(&charger.station_id)
        .bind(charger.connector_type_id)
        .bind(charger.status_id)
        .bind(charger.current_type_id)
        .bind(charger.power_kw)
        .bind(charger.voltage)
        .bind(charger.amperage)
        .bind(charger.count_available)
        .bind(charger.count_total)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn find_by_id(&self, charger_id: &str) -> Result<Charger, ServiceError> {
        let row = sqlx::query_as::<_, ChargerRow>(
            r#"SELECT charger_id, station_id, connector_type_id, status_id, current_type_id,
                      power_kw::double precision, voltage, amperage, count_available, count_total,
                      created_by_uuid, updated_by_uuid,
                      created_at, updated_at, deleted_at
               FROM ev.chargers
               WHERE charger_id = $1 AND deleted_at IS NULL"#,
        )
        .bind(charger_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|r| r.into())
            .ok_or_else(|| ServiceError::NotFound(format!("charger {}", charger_id)))
    }

    pub async fn list(&self, limit: i64, offset: i64, station_id: Option<&str>) -> Result<Vec<Charger>, ServiceError> {
        let rows = sqlx::query_as::<_, ChargerRow>(
            r#"SELECT charger_id, station_id, connector_type_id, status_id, current_type_id,
                      power_kw::double precision, voltage, amperage, count_available, count_total,
                      created_by_uuid, updated_by_uuid,
                      created_at, updated_at, deleted_at
                FROM ev.chargers
                WHERE deleted_at IS NULL
                  AND ($1::text IS NULL OR station_id = $1)
                ORDER BY created_at DESC
                LIMIT $2 OFFSET $3"#,
        )
        .bind(station_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.into()).collect())
    }

    pub async fn count(&self, station_id: Option<&str>) -> Result<i64, ServiceError> {
        let row = sqlx::query_scalar::<_, i64>(
            r#"SELECT COUNT(*) FROM ev.chargers
               WHERE deleted_at IS NULL
                 AND ($1::text IS NULL OR station_id = $1)"#,
        )
        .bind(station_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn update(
        &self,
        charger_id: &str,
        connector_type_id: i32,
        status_id: i32,
        current_type_id: i32,
        power_kw: Option<f64>,
        voltage: Option<i32>,
        amperage: Option<i32>,
        count_available: i32,
        count_total: i32,
    ) -> Result<Charger, ServiceError> {
        sqlx::query(
            r#"UPDATE ev.chargers
               SET connector_type_id = $1, status_id = $2, current_type_id = $3,
                   power_kw = $4, voltage = $5, amperage = $6,
                   count_available = $7, count_total = $8, updated_at = NOW()
               WHERE charger_id = $9 AND deleted_at IS NULL"#,
        )
        .bind(connector_type_id)
        .bind(status_id)
        .bind(current_type_id)
        .bind(power_kw)
        .bind(voltage)
        .bind(amperage)
        .bind(count_available)
        .bind(count_total)
        .bind(charger_id)
        .execute(&self.pool)
        .await?;

        self.find_by_id(charger_id).await
    }

    pub async fn soft_delete(&self, charger_id: &str) -> Result<(), ServiceError> {
        let affected = sqlx::query(
            r#"UPDATE ev.chargers SET deleted_at = NOW()
               WHERE charger_id = $1 AND deleted_at IS NULL"#,
        )
        .bind(charger_id)
        .execute(&self.pool)
        .await?
        .rows_affected();

        if affected == 0 {
            return Err(ServiceError::NotFound(format!("charger {}", charger_id)));
        }
        Ok(())
    }
}

#[derive(sqlx::FromRow)]
struct ChargerRow {
    charger_id: String,
    station_id: String,
    connector_type_id: i32,
    status_id: i32,
    current_type_id: i32,
    power_kw: Option<f64>,
    voltage: Option<i32>,
    amperage: Option<i32>,
    count_available: i32,
    count_total: i32,
    created_by_uuid: Option<uuid::Uuid>,
    updated_by_uuid: Option<uuid::Uuid>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: Option<chrono::DateTime<chrono::Utc>>,
    deleted_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<ChargerRow> for Charger {
    fn from(r: ChargerRow) -> Self {
        Self {
            charger_id: r.charger_id,
            station_id: r.station_id,
            connector_type_id: r.connector_type_id,
            status_id: r.status_id,
            current_type_id: r.current_type_id,
            power_kw: r.power_kw,
            voltage: r.voltage,
            amperage: r.amperage,
            count_available: r.count_available,
            count_total: r.count_total,
            created_by_uuid: r.created_by_uuid,
            updated_by_uuid: r.updated_by_uuid,
            created_at: r.created_at,
            updated_at: r.updated_at,
            deleted_at: r.deleted_at,
        }
    }
}
