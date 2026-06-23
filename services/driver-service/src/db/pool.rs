use sqlx::PgPool;

/// Wrapper type for platform_db (users, gis, inventory)
#[derive(Clone)]
pub struct PlatformDb(pub PgPool);

/// Wrapper type for analytics_db (telemetry, analytics, system)
#[derive(Clone)]
pub struct AnalyticsDb(pub PgPool);
