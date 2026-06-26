use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use std::sync::Arc;
use tracing::{error, info, instrument};

#[derive(Clone)]
pub struct Cache {
    conn: Arc<ConnectionManager>,
}

impl Cache {
    /// Create a new Cache instance from a connection string
    #[instrument(skip(conn_str))]
    pub async fn from_connection_string(conn_str: &str) -> Result<Self, redis::RedisError> {
        info!("Connecting to Redis...");
        let client = redis::Client::open(conn_str)?;
        let manager = client.get_async_connection().await?;
        info!("Redis connection established successfully");

        // Test the connection
        let _: () = manager.ping().await?;

        Ok(Cache {
            conn: Arc::new(manager),
        })
    }

    /// Get a reference to the connection manager
    pub fn get_conn(&self) -> &Arc<ConnectionManager> {
        &self.conn
    }

    /// Create a cache instance from an existing connection manager
    pub fn from_connection(conn: Arc<ConnectionManager>) -> Self {
        Cache { conn }
    }

    /// Check if Redis is connected
    pub fn is_connected(&self) -> bool {
        self.conn.status().is_connected()
    }

    /// Set a key-value pair with TTL
    #[instrument(skip(self, key, value))]
    pub async fn set_with_ttl(
        &self,
        key: &str,
        value: &str,
        ttl: u64,
    ) -> Result<(), redis::RedisError> {
        info!("Setting key {} with TTL {}", key, ttl);
        let _: () = self.conn.set_ex(key, value, ttl).await?;
        Ok(())
    }

    /// Get a value by key
    #[instrument(skip(self, key))]
    pub async fn get(&self, key: &str) -> Result<Option<String>, redis::RedisError> {
        let result: Option<String> = self.conn.get(key).await?;
        if let Some(ref value) = result {
            info!("Retrieved value for key {}", key);
        }
        Ok(result)
    }

    /// Delete a key
    #[instrument(skip(self, key))]
    pub async fn del(&self, key: &str) -> Result<(), redis::RedisError> {
        info!("Deleting key {}", key);
        let _: () = self.conn.del(key).await?;
        Ok(())
    }

    /// Check if a key exists
    #[instrument(skip(self, key))]
    pub async fn exists(&self, key: &str) -> Result<bool, redis::RedisError> {
        let result: bool = self.conn.exists(key).await?;
        Ok(result)
    }

    /// Increment a counter
    #[instrument(skip(self, key))]
    pub async fn incr(&self, key: &str) -> Result<u64, redis::RedisError> {
        let result: u64 = self.conn.incr(key, 1).await?;
        info!("Incremented counter {} to {}", key, result);
        Ok(result)
    }

    /// Decrement a counter
    #[instrument(skip(self, key))]
    pub async fn decr(&self, key: &str) -> Result<u64, redis::RedisError> {
        let result: u64 = self.conn.decr(key, 1).await?;
        info!("Decremented counter {} to {}", key, result);
        Ok(result)
    }

    /// Set expiration on a key
    #[instrument(skip(self, key, ttl))]
    pub async fn expire(&self, key: &str, ttl: u64) -> Result<bool, redis::RedisError> {
        let result: bool = self.conn.expire(key, ttl).await?;
        info!("Set expiration {} on key {}", ttl, key);
        Ok(result)
    }

    /// Get TTL for a key
    #[instrument(skip(self, key))]
    pub async fn ttl(&self, key: &str) -> Result<i64, redis::RedisError> {
        let result: i64 = self.conn.ttl(key).await?;
        info!("TTL for key {} is {} seconds", key, result);
        Ok(result)
    }

    /// Hash operations
    #[instrument(skip(self))]
    pub async fn hset(&self, key: &str, field: &str, value: &str) -> Result<(), redis::RedisError> {
        let _: () = self.conn.hset(key, field, value).await?;
        Ok(())
    }

    pub async fn hget(&self, key: &str, field: &str) -> Result<Option<String>, redis::RedisError> {
        let result: Option<String> = self.conn.hget(key, field).await?;
        Ok(result)
    }

    pub async fn hgetall(
        &self,
        key: &str,
    ) -> Result<redis::RedisResult<redis::HashMap<String, String>>, redis::RedisError> {
        let result: redis::HashMap<String, String> = self.conn.hgetall(key).await?;
        Ok(result)
    }

    /// List operations
    pub async fn lpush(&self, key: &str, values: Vec<&str>) -> Result<u64, redis::RedisError> {
        let result: u64 = self.conn.lpush(key, values).await?;
        Ok(result)
    }

    pub async fn rpush(&self, key: &str, values: Vec<&str>) -> Result<u64, redis::RedisError> {
        let result: u64 = self.conn.rpush(key, values).await?;
        Ok(result)
    }

    pub async fn lpop(&self, key: &str) -> Result<Option<String>, redis::RedisError> {
        let result: Option<String> = self.conn.lpop(key).await?;
        Ok(result)
    }

    pub async fn rpop(&self, key: &str) -> Result<Option<String>, redis::RedisError> {
        let result: Option<String> = self.conn.rpop(key).await?;
        Ok(result)
    }

    pub async fn llen(&self, key: &str) -> Result<u64, redis::RedisError> {
        let result: u64 = self.conn.llen(key).await?;
        Ok(result)
    }

    pub async fn lrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<String>, redis::RedisError> {
        let result: Vec<String> = self.conn.lrange(key, start, stop).await?;
        Ok(result)
    }
}

impl Default for Cache {
    fn default() -> Self {
        panic!("Cache::default() requires explicit connection string");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_creation() {
        let cache = Cache::from_connection(Arc::new(
            ConnectionManager::new(redis::Client::open("redis://127.0.0.1").unwrap()).unwrap(),
        ));
        assert!(!cache.is_connected());
    }

    #[tokio::test]
    async fn test_cache_arc_clone() {
        let conn =
            ConnectionManager::new(redis::Client::open("redis://127.0.0.1").unwrap()).unwrap();
        let cache1 = Cache::from_connection(Arc::new(conn));
        let cache2 = cache1.clone();
        assert!(Arc::ptr_eq(&cache1.conn, &cache2.conn));
    }
}
