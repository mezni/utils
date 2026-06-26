use shared_cache::Cache;

pub struct CacheInfrastructure {
    cache: Cache,
}

impl CacheInfrastructure {
    pub fn new(cache: Cache) -> Self {
        CacheInfrastructure { cache }
    }

    pub async fn set_token_blacklist(&self, jti: uuid::Uuid, token: &str, ttl: u64) -> Result<(), redis::RedisError> {
        let key = format!("jti_blacklist:{}", jti);
        self.cache.set_with_ttl(&key, token, ttl).await
    }

    pub async fn get_token_blacklist(&self, jti: uuid::Uuid) -> Result<Option<String>, redis::RedisError> {
        let key = format!("jti_blacklist:{}", jti);
        self.cache.get(&key).await
    }

    pub async fn remove_token_blacklist(&self, jti: uuid::Uuid) -> Result<(), redis::RedisError> {
        let key = format!("jti_blacklist:{}", jti);
        self.cache.del(&key).await
    }

    pub async fn increment_rate_limit(&self, key: &str) -> Result<u64, redis::RedisError> {
        self.cache.incr(key).await
    }

    pub async fn decrement_rate_limit(&self, key: &str) -> Result<u64, redis::RedisError> {
        self.cache.decr(key).await
    }

    pub async fn get_rate_limit(&self, key: &str) -> Result<Option<u64>, redis::RedisError> {
        let count: Option<u64> = self.cache.get(key).await?;
        Ok(count)
    }

    pub async fn set_rate_limit_expiry(&self, key: &str, ttl: u64) -> Result<bool, redis::RedisError> {
        self.cache.expire(key, ttl).await
    }
}