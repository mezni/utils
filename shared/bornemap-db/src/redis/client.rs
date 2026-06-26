use std::sync::Arc;
use tokio::sync::RwLock;
use redis::{aio::ConnectionManager, Client, ConnectionLike, RedisError, RedisResult};
use crate::redis::{RedisError as RedisErrorType, RedisResult as RedisResultType};

pub struct RedisClient {
    client: Client,
    connection: Arc<RwLock<Option<ConnectionManager>>>,
    max_retries: u32,
    retry_delay_ms: u64,
}

impl RedisClient {
    /// Create a new Redis client with the given URL
    pub fn new(redis_url: &str) -> RedisResultType<Self> {
        let client = Client::open(redis_url)
            .map_err(|e| RedisErrorType::Connection(e.to_string()))?;

        Ok(Self {
            client,
            connection: Arc::new(RwLock::new(None)),
            max_retries: 3,
            retry_delay_ms: 100,
        })
    }

    /// Create a new Redis client with custom configuration
    pub fn with_config(redis_url: &str, max_retries: u32, retry_delay_ms: u64) -> RedisResultType<Self> {
        let client = Client::open(redis_url)
            .map_err(|e| RedisErrorType::Connection(e.to_string()))?;

        Ok(Self {
            client,
            connection: Arc::new(RwLock::new(None)),
            max_retries,
            retry_delay_ms,
        })
    }

    /// Initialize the connection to Redis
    pub async fn initialize(&self) -> RedisResultType<()> {
        let mut connection_guard = self.connection.write().await;
        
        if connection_guard.is_none() {
            let mut conn = self.client.get_async_connection()
                .await
                .map_err(|e| RedisErrorType::Connection(e.to_string()))?;
            
            // Test the connection
            let _: redis::RedisResult<()> = redis::cmd("PING").query_async(&mut conn).await
                .map_err(|e| RedisErrorType::Connection(e.to_string()))?;
            
            *connection_guard = Some(conn);
        }
        
        Ok(())
    }

    /// Get a reference to the Redis connection
    async fn get_connection(&self) -> RedisResultType<ConnectionManager> {
        let connection_guard = self.connection.read().await;
        
        if let Some(conn) = connection_guard.as_ref() {
            // Clone the connection manager for use
            Ok(conn.clone())
        } else {
            drop(connection_guard);
            // Try to initialize and get connection
            self.initialize().await?;
            
            let connection_guard = self.connection.read().await;
            connection_guard.as_ref()
                .cloned()
                .ok_or_else(|| RedisErrorType::Connection("Connection not available".to_string()))
        }
    }

    /// Execute a Redis command with automatic retry
    async fn execute_with_retry<F, T>(&self, mut operation: F) -> RedisResultType<T>
    where
        F: FnMut(&mut ConnectionManager) -> RedisResult<T>,
    {
        let mut last_error = None;
        
        for attempt in 0..=self.max_retries {
            match self.get_connection().await {
                Ok(mut conn) => {
                    match operation(&mut conn) {
                        Ok(result) => return Ok(result),
                        Err(e) => {
                            last_error = Some(e);
                            // If we have more retries, try to reconnect
                            if attempt < self.max_retries {
                                // Reset connection and try again
                                let mut connection_guard = self.connection.write().await;
                                *connection_guard = None;
                                tokio::time::sleep(tokio::time::Duration::from_millis(self.retry_delay_ms)).await;
                            }
                        }
                    }
                }
                Err(e) => {
                    last_error = Some(e);
                    if attempt < self.max_retries {
                        tokio::time::sleep(tokio::time::Duration::from_millis(self.retry_delay_ms)).await;
                    }
                }
            }
        }
        
        Err(last_error.unwrap_or_else(|| RedisErrorType::Command("Operation failed after retries".to_string())))
    }

    /// Set a key with value and TTL
    pub async fn set_with_ttl(&self, key: &str, value: &str, ttl_seconds: u64) -> RedisResultType<()> {
        self.execute_with_retry(|conn| {
            redis::cmd("SETEX")
                .arg(key)
                .arg(ttl_seconds)
                .arg(value)
                .query_async(conn)
                .await
        }).await
        .map_err(|e| RedisErrorType::Command(format!("Failed to set key with TTL: {}", e)))
    }

    /// Get a value by key
    pub async fn get(&self, key: &str) -> RedisResultType<Option<String>> {
        self.execute_with_retry(|conn| {
            redis::cmd("GET")
                .arg(key)
                .query_async(conn)
                .await
        }).await
        .map_err(|e| RedisErrorType::Command(format!("Failed to get key: {}", e)))
    }

    /// Delete a key
    pub async fn delete(&self, key: &str) -> RedisResultType<()> {
        self.execute_with_retry(|conn| {
            redis::cmd("DEL")
                .arg(key)
                .query_async(conn)
                .await
        }).await
        .map_err(|e| RedisErrorType::Command(format!("Failed to delete key: {}", e)))
    }

    /// Check if a key exists
    pub async fn exists(&self, key: &str) -> RedisResultType<bool> {
        self.execute_with_retry(|conn| {
            redis::cmd("EXISTS")
                .arg(key)
                .query_async(conn)
                .await
        }).await
        .map_err(|e| RedisErrorType::Command(format!("Failed to check key existence: {}", e)))
    }

    /// Increment a key and return the new value
    pub async fn increment(&self, key: &str) -> RedisResultType<i64> {
        self.execute_with_retry(|conn| {
            redis::cmd("INCR")
                .arg(key)
                .query_async(conn)
                .await
        }).await
        .map_err(|e| RedisErrorType::Command(format!("Failed to increment key: {}", e)))
    }

    /// Set a key with expiration only if it doesn't exist
    pub async fn set_if_not_exists(&self, key: &str, value: &str, ttl_seconds: u64) -> RedisResultType<bool> {
        self.execute_with_retry(|conn| {
            // Use SET with NX option and EXPIRE
            let result: RedisResult<bool> = redis::cmd("SET")
                .arg(key)
                .arg(value)
                .arg("NX")
                .arg("EX")
                .arg(ttl_seconds)
                .query_async(conn)
                .await;
            
            match result {
                Ok(result) => Ok(result),
                Err(e) => {
                    // If key exists, return false
                    if e.is::<redis::Error>() && e.to_string().contains("exists") {
                        Ok(false)
                    } else {
                        Err(e)
                    }
                }
            }
        }).await
        .map_err(|e| RedisErrorType::Command(format!("Failed to set key if not exists: {}", e)))
    }

    /// Get the TTL of a key
    pub async fn ttl(&self, key: &str) -> RedisResultType<i64> {
        self.execute_with_retry(|conn| {
            redis::cmd("TTL")
                .arg(key)
                .query_async(conn)
                .await
        }).await
        .map_err(|e| RedisErrorType::Command(format!("Failed to get TTL: {}", e)))
    }

    /// Check if a key exists and has not expired
    pub async fn exists_and_valid(&self, key: &str) -> RedisResultType<bool> {
        match self.ttl(key).await {
            Ok(ttl) => Ok(ttl > 0),
            Err(e) => {
                // If key doesn't exist, return false
                if e.to_string().contains("not found") {
                    Ok(false)
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Close the Redis connection
    pub async fn close(&self) -> RedisResultType<()> {
        let mut connection_guard = self.connection.write().await;
        *connection_guard = None;
        Ok(())
    }

    /// Health check - ping Redis server
    pub async fn health_check(&self) -> RedisResultType<()> {
        self.execute_with_retry(|conn| {
            redis::cmd("PING")
                .query_async(conn)
                .await
        }).await
        .map_err(|e| RedisErrorType::Command(format!("Health check failed: {}", e)))
    }
}

impl Clone for RedisClient {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            connection: self.connection.clone(),
            max_retries: self.max_retries,
            retry_delay_ms: self.retry_delay_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // Mock Redis client for testing
    struct MockRedisClient {
        should_fail: bool,
    }

    impl MockRedisClient {
        fn new() -> Self {
            Self { should_fail: false }
        }

        fn fail_next(mut self) -> Self {
            self.should_fail = true;
            self
        }
    }

    impl MockRedisClient {
        async fn set_with_ttl(&self, key: &str, value: &str, _ttl_seconds: u64) -> RedisResultType<()> {
            if self.should_fail {
                return Err(RedisErrorType::Command("Mock failure".to_string()));
            }
            println!("Mock set: {} = {}", key, value);
            Ok(())
        }

        async fn get(&self, key: &str) -> RedisResultType<Option<String>> {
            if self.should_fail {
                return Err(RedisErrorType::Command("Mock failure".to_string()));
            }
            if key == "test_key" {
                Ok(Some("test_value".to_string()))
            } else {
                Ok(None)
            }
        }

        async fn delete(&self, key: &str) -> RedisResultType<()> {
            if self.should_fail {
                return Err(RedisErrorType::Command("Mock failure".to_string()));
            }
            println!("Mock delete: {}", key);
            Ok(())
        }

        async fn exists(&self, key: &str) -> RedisResultType<bool> {
            if self.should_fail {
                return Err(RedisErrorType::Command("Mock failure".to_string()));
            }
            Ok(key == "test_key")
        }

        async fn increment(&self, key: &str) -> RedisResultType<i64> {
            if self.should_fail {
                return Err(RedisErrorType::Command("Mock failure".to_string()));
            }
            println!("Mock increment: {}", key);
            Ok(1)
        }
    }

    #[tokio::test]
    async fn test_mock_redis_operations() {
        let client = MockRedisClient::new();

        // Test set with TTL
        let result = client.set_with_ttl("test_key", "test_value", 300).await;
        assert!(result.is_ok());

        // Test get
        let result = client.get("test_key").await;
        assert_eq!(result.unwrap(), Some("test_value".to_string()));

        // Test delete
        let result = client.delete("test_key").await;
        assert!(result.is_ok());

        // Test exists
        let result = client.exists("test_key").await;
        assert!(result.unwrap());

        // Test increment
        let result = client.increment("counter").await;
        assert_eq!(result.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_mock_redis_failure() {
        let client = MockRedisClient::new().fail_next();

        // Test failure
        let result = client.set_with_ttl("test_key", "test_value", 300).await;
        assert!(result.is_err());
    }
}