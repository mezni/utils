use actix_web::web;
use redis::aio::ConnectionManager;
use tracing::{error, info, warn};

pub async fn init_redis_pool(redis_url: &str) -> Result<ConnectionManager, redis::RedisError> {
    let client = redis::Client::open(redis_url)?;
    let conn = ConnectionManager::new(client).await?;
    info!("Redis client initialized successfully");
    Ok(conn)
}

pub async fn store_idempotency_key(
    redis_conn: &mut ConnectionManager,
    key: &str,
    response_data: &str,
    ttl_seconds: u64,
) -> Result<(), redis::RedisError> {
    let result = redis_conn
        .set_ex(key, response_data, ttl_seconds)
        .await?;

    match result {
        true => info!("Idempotency key stored: {}", key),
        false => warn!("Failed to store idempotency key: {}", key),
    }

    Ok(())
}

pub async fn get_idempotency_key(
    redis_conn: &mut ConnectionManager,
    key: &str,
) -> Result<Option<String>, redis::RedisError> {
    let result = redis_conn.get(key).await?;

    match result {
        Some(value) => {
            info!("Idempotency key retrieved: {} (replay detected)", key);
            Ok(Some(value))
        }
        None => {
            info!("Idempotency key not found: {} (new request)", key);
            Ok(None)
        }
    }
}

pub async fn delete_idempotency_key(
    redis_conn: &mut ConnectionManager,
    key: &str,
) -> Result<(), redis::RedisError> {
    redis_conn.del(key).await?;
    info!("Idempotency key deleted: {}", key);
    Ok(())
}

pub async fn invalidate_cache_pattern(
    redis_conn: &mut ConnectionManager,
    pattern: &str,
) -> Result<u64, redis::RedisError> {
    let keys: Vec<String> = redis_conn
        .keys(format!("{}*", pattern))
        .await?
        .into_iter()
        .filter_map(|key| key.ok())
        .collect();

    if keys.is_empty() {
        warn!("No cache keys found for pattern: {}", pattern);
        return Ok(0);
    }

    let mut deleted = 0;
    for key in keys {
        match redis_conn.del(&key).await {
            Ok(_) => {
                deleted += 1;
                if deleted % 100 == 0 {
                    info!("Deleted {} cache keys for pattern: {}", deleted, pattern);
                }
            }
            Err(e) => {
                error!("Failed to delete cache key {}: {}", key, e);
            }
        }
    }

    info!("Invalidated {} cache keys for pattern: {}", deleted, pattern);
    Ok(deleted)
}
