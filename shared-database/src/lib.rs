use sqlx::{PgPool, Pool, Postgres};
use std::sync::Arc;
use tracing::{error, info, instrument};

pub type DatabasePool = Pool<Postgres>;

#[derive(Clone)]
pub struct Database {
    pool: Arc<Pool<Postgres>>,
}

impl Database {
    /// Create a new Database connection from a connection string
    #[instrument(skip(conn_str))]
    pub async fn from_connection_string(conn_str: &str) -> Result<Self, sqlx::Error> {
        info!("Connecting to database...");
        let pool = PgPool::connect(conn_str).await?;
        info!("Database connection established successfully");

        // Test the connection
        sqlx::query("SELECT 1").fetch_one(&pool).await?;

        Ok(Database {
            pool: Arc::new(pool),
        })
    }

    /// Get a reference to the pool
    pub fn get_pool(&self) -> &Pool<Postgres> {
        &self.pool
    }

    /// Get a reference to the pool as a reference
    pub fn get_pool_ref(&self) -> &Pool<Postgres> {
        &self.pool
    }

    /// Get a mutable reference to the pool
    pub fn get_pool_mut(&mut self) -> &mut Pool<Postgres> {
        &mut self.pool
    }

    /// Create a database instance from an existing pool
    pub fn from_pool(pool: Pool<Postgres>) -> Self {
        Database {
            pool: Arc::new(pool),
        }
    }

    /// Create a database instance with an Arc<Pool>
    pub fn from_arc_pool(pool: Arc<Pool<Postgres>>) -> Self {
        Database { pool }
    }

    /// Execute a query within a transaction
    #[instrument(skip(self, query))]
    pub async fn transaction<F, T, E>(&self, query: F) -> Result<T, E>
    where
        F: for<'a> sqlx::Transaction<'a, Postgres>::Execute + Send + Sync,
        T: for<'a> sqlx::FromRow<'a, sqlx::Row> + Send + Sync,
        E: std::error::Error + Send + Sync + 'static,
    {
        let mut tx = self.pool.begin().await?;
        let result = query.execute(&mut tx).await?;
        tx.commit().await?;
        Ok(sqlx::FromRow::from_row(&result))
    }

    /// Get the number of connections
    pub fn get_pool_size(&self) -> u32 {
        self.pool.max_size() as u32
    }

    /// Check if the pool is connected
    pub fn is_connected(&self) -> bool {
        self.pool.get_ref().status().is_connected()
    }
}

impl Default for Database {
    fn default() -> Self {
        panic!("Database::default() requires explicit connection string");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_database_creation() {
        let db = Database::from_pool(PgPool::new(None).await.unwrap());
        assert_eq!(db.get_pool_size(), 0);
        assert!(!db.is_connected());
    }

    #[tokio::test]
    async fn test_database_arc_clone() {
        let pool = PgPool::new(None).await.unwrap();
        let db1 = Database::from_arc_pool(Arc::new(pool.clone()));
        let db2 = db1.clone();
        assert!(Arc::ptr_eq(&db1.pool, &db2.pool));
    }
}
