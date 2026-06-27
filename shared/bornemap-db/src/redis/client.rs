use redis::{Client, cmd};
use crate::redis::{RedisError as RedisErrorType, RedisResult as RedisResultType};

#[derive(Clone)]
pub struct RedisClient {
    client: Client,
}

impl RedisClient {
    /// Create a new Redis client with the given URL
    pub fn new(redis_url: &str) -> RedisResultType<Self> {
        let client = Client::open(redis_url)
            .map_err(|e| RedisErrorType::Connection(e.to_string()))?;

        Ok(Self { client })
    }

    /// Set a key with value and TTL
    pub async fn set_with_ttl(&self, key: &str, value: &str, ttl_seconds: u64) -> RedisResultType<()> {
        let mut conn = self.client.get_connection()
            .map_err(|e| RedisErrorType::Connection(e.to_string()))?;
        
        cmd("SETEX")
            .arg(key)
            .arg(ttl_seconds)
            .arg(value)
            .query(&mut conn)
            .map_err(|e| RedisErrorType::Command(e.to_string()))
    }

    /// Get a value by key
    pub async fn get(&self, key: &str) -> RedisResultType<Option<String>> {
        let mut conn = self.client.get_connection()
            .map_err(|e| RedisErrorType::Connection(e.to_string()))?;
        
        cmd("GET")
            .arg(key)
            .query(&mut conn)
            .map_err(|e| RedisErrorType::Command(e.to_string()))
    }

    /// Delete a key
    pub async fn delete(&self, key: &str) -> RedisResultType<u64> {
        let mut conn = self.client.get_connection()
            .map_err(|e| RedisErrorType::Connection(e.to_string()))?;
        
        cmd("DEL")
            .arg(key)
            .query(&mut conn)
            .map_err(|e| RedisErrorType::Command(e.to_string()))
    }

    /// Check if a key exists
    pub async fn exists(&self, key: &str) -> RedisResultType<bool> {
        let mut conn = self.client.get_connection()
            .map_err(|e| RedisErrorType::Connection(e.to_string()))?;
        
        cmd("EXISTS")
            .arg(key)
            .query(&mut conn)
            .map_err(|e| RedisErrorType::Command(e.to_string()))
    }

    /// Increment a counter
    pub async fn increment(&self, key: &str) -> RedisResultType<u64> {
        let mut conn = self.client.get_connection()
            .map_err(|e| RedisErrorType::Connection(e.to_string()))?;
        
        cmd("INCR")
            .arg(key)
            .query(&mut conn)
            .map_err(|e| RedisErrorType::Command(e.to_string()))
    }

    /// Set a key with value (no TTL)
    pub async fn set(&self, key: &str, value: &str) -> RedisResultType<()> {
        let mut conn = self.client.get_connection()
            .map_err(|e| RedisErrorType::Connection(e.to_string()))?;
        
        cmd("SET")
            .arg(key)
            .arg(value)
            .query(&mut conn)
            .map_err(|e| RedisErrorType::Command(e.to_string()))
    }

    /// Get TTL for a key
    pub async fn ttl(&self, key: &str) -> RedisResultType<i64> {
        let mut conn = self.client.get_connection()
            .map_err(|e| RedisErrorType::Connection(e.to_string()))?;
        
        cmd("TTL")
            .arg(key)
            .query(&mut conn)
            .map_err(|e| RedisErrorType::Command(e.to_string()))
    }

    /// Check if a key exists and handle "no such key" errors
    pub async fn key_exists(&self, key: &str) -> RedisResultType<bool> {
        match self.exists(key).await {
            Ok(exists) => Ok(exists),
            Err(e) => {
                // Check if it's a "no such key" error
                if e.to_string().contains("no such key") || e.to_string().contains("not found") {
                    Ok(false)
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Ping the Redis server
    pub async fn ping(&self) -> RedisResultType<String> {
        let mut conn = self.client.get_connection()
            .map_err(|e| RedisErrorType::Connection(e.to_string()))?;
        
        cmd("PING")
            .query(&mut conn)
            .map_err(|e| RedisErrorType::Command(e.to_string()))
    }
}