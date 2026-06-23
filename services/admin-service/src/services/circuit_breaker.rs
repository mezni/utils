//! Circuit breaker implementation for admin-service
//! Provides retry logic with exponential backoff and state management

use anyhow::{Context, Result};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tokio::sync::Mutex;

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Circuit is closed - allow requests
    Closed,
    /// Circuit is open - reject requests
    Open,
    /// Circuit is half-open - allow one trial request
    HalfOpen,
}

/// Circuit breaker configuration
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Maximum number of consecutive failures before opening the circuit
    pub max_failure_count: u32,
    /// Duration to stay in open state before trying again
    pub open_duration: Duration,
    /// Duration for half-open state to stay before trying again
    pub half_open_duration: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            max_failure_count: 5,
            open_duration: Duration::from_secs(30),
            half_open_duration: Duration::from_secs(1),
        }
    }
}

/// Circuit breaker metrics
#[derive(Debug, Clone)]
pub struct CircuitBreakerMetrics {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub state: State,
}

impl CircuitBreakerMetrics {
    pub fn success_rate(&self) -> f64 {
        if self.total_requests == 0 {
            1.0
        } else {
            self.successful_requests as f64 / self.total_requests as f64
        }
    }
}

/// Circuit breaker for handling failures and retries
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    failure_count: AtomicU32,
    last_failure_time: std::sync::Arc<std::sync::Mutex<Option<Instant>>>,
    state: std::sync::Arc<Mutex<State>>,
    metrics: std::sync::Arc<tokio::sync::Mutex<CircuitBreakerMetrics>>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with configuration
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            failure_count: AtomicU32::new(0),
            last_failure_time: std::sync::Arc::new(std::sync::Mutex::new(None)),
            state: std::sync::Arc::new(Mutex::new(State::Closed)),
            metrics: std::sync::Arc::new(tokio::sync::Mutex::new(CircuitBreakerMetrics {
                total_requests: 0,
                successful_requests: 0,
                failed_requests: 0,
                state: State::Closed,
            })),
        }
    }

    /// Create default circuit breaker
    pub fn default() -> Self {
        Self::new(CircuitBreakerConfig::default())
    }

    /// Execute a function with circuit breaker protection
    ///
    /// # Arguments
    /// * `f` - Function to execute
    ///
    /// # Returns
    /// Result of the function if successful, Error if circuit is open
    pub async fn call<F, T, E>(&self, f: F) -> Result<T, CircuitBreakerError>
    where
        F: FnOnce() -> Result<T, E>,
        E: std::fmt::Debug + Send + Sync + 'static,
    {
        let mut state = self.state.lock().await;

        // Check if circuit is open
        match *state {
            State::Open => {
                // Check if circuit can be closed
                if self.can_close().await {
                    *state = State::HalfOpen;
                } else {
                    return Err(CircuitBreakerError::CircuitOpen {
                        message: "Circuit breaker is open, rejecting request".to_string(),
                    });
                }
            }
            State::HalfOpen => {
                // Allow one trial request
            }
            State::Closed => {}
        }

        // Execute the function
        let result = f();
        let total_requests = self.metrics.total_requests.fetch_add(1, Ordering::Relaxed) + 1;

        match result {
            Ok(value) => {
                self.on_success();
                Ok(value)
            }
            Err(e) => {
                self.on_failure(e);
                Err(CircuitBreakerError::OperationFailed {
                    error: format!("{:?}", e),
                })
            }
        }
    }

    /// Handle successful request
    fn on_success(&self) {
        let mut metrics = self.metrics.lock().await;
        metrics.successful_requests += 1;
        metrics.state = State::Closed;

        // Reset failure count on success
        self.failure_count.store(0, Ordering::Relaxed);
    }

    /// Handle failed request
    fn on_failure(&self, error: impl std::fmt::Debug) {
        let mut state = self.state.lock().await;
        let mut metrics = self.metrics.lock().await;

        metrics.failed_requests += 1;
        metrics.state = *state;

        // Increment failure count
        let new_failure_count = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;

        // Check if circuit should open
        if new_failure_count >= self.config.max_failure_count {
            *state = State::Open;
            let mut last_failure = self.last_failure_time.lock().unwrap();
            *last_failure = Some(Instant::now());
            eprintln!("⚠️ Circuit breaker opened after {} failures: {:?}", new_failure_count, error);
        }
    }

    /// Check if circuit can be closed
    async fn can_close(&self) -> bool {
        let last_failure = self.last_failure_time.lock().unwrap();
        let time_since_last_failure = match *last_failure {
            Some(time) => time.elapsed(),
            None => return false,
        };

        time_since_last_failure >= self.config.open_duration
    }

    /// Reset the circuit breaker (useful for testing)
    pub async fn reset(&self) {
        self.failure_count.store(0, Ordering::Relaxed);
        let mut last_failure = self.last_failure_time.lock().unwrap();
        *last_failure = None;

        let mut state = self.state.lock().await;
        *state = State::Closed;

        let mut metrics = self.metrics.lock().await;
        metrics.state = State::Closed;

        eprintln!("✅ Circuit breaker reset to Closed state");
    }

    /// Get current state
    pub async fn state(&self) -> State {
        *self.state.lock().await
    }

    /// Get metrics
    pub async fn metrics(&self) -> CircuitBreakerMetrics {
        self.metrics.lock().await.clone()
    }
}

/// Errors that can occur with circuit breaker
#[derive(Debug, thiserror::Error)]
pub enum CircuitBreakerError {
    #[error("Circuit breaker is open, rejecting request: {message}")]
    CircuitOpen { message: String },
    #[error("Operation failed: {error}")]
    OperationFailed { error: String },
    #[error("Timeout waiting for circuit to close")]
    Timeout,
}

/// Retry with exponential backoff
///
/// # Arguments
/// * `f` - Function to execute
/// * `max_retries` - Maximum number of retries
/// * `initial_delay` - Initial delay between retries
/// * `max_delay` - Maximum delay between retries
/// * `backoff_multiplier` - Multiplier for exponential backoff
///
/// # Returns
/// Result of the function if successful after all retries
pub async fn retry_with_exponential_backoff<F, T, E>(
    f: F,
    max_retries: u32,
    initial_delay: Duration,
    max_delay: Duration,
    backoff_multiplier: f64,
) -> Result<T, CircuitBreakerError>
where
    F: Fn() -> Result<T, E>,
    E: std::fmt::Debug + Send + Sync + 'static,
{
    let mut delay = initial_delay;

    for attempt in 0..max_retries {
        match f() {
            Ok(value) => {
                if attempt > 0 {
                    eprintln!("✅ Operation succeeded after {} retries", attempt);
                }
                return Ok(value);
            }
            Err(e) => {
                if attempt < max_retries - 1 {
                    eprintln!("⚠️ Attempt {} failed, retrying in {:?}...", attempt + 1, delay);
                    sleep(delay).await;
                    delay = (delay.as_secs_f64() * backoff_multiplier) as u64;
                    if delay > max_delay.as_secs() {
                        delay = max_delay;
                    }
                } else {
                    eprintln!("❌ All {} attempts failed: {:?}", max_retries, e);
                    return Err(CircuitBreakerError::OperationFailed {
                        error: format!("{:?}", e),
                    });
                }
            }
        }
    }

    Err(CircuitBreakerError::Timeout)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_circuit_breaker_closed_state() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            max_failure_count: 3,
            open_duration: Duration::from_secs(30),
            half_open_duration: Duration::from_secs(1),
        });

        // Should allow requests when closed
        let result = cb.call(|| Ok::<(), ()>(())).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_circuit_breaker_opens_on_failures() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            max_failure_count: 2,
            open_duration: Duration::from_secs(1),
            half_open_duration: Duration::from_secs(1),
        });

        // Allow requests
        cb.call(|| Err::<(), ()>(())).await.unwrap_err();
        cb.call(|| Err::<(), ()>(())).await.unwrap_err();

        // Circuit should be open
        let state = cb.state().await;
        assert_eq!(state, State::Open);
    }

    #[tokio::test]
    async fn test_retry_with_exponential_backoff() {
        let attempts = std::sync::Arc::new(std::sync::Mutex::new(0));

        let result = retry_with_exponential_backoff(
            || {
                *attempts.lock().unwrap() += 1;
                if **attempts.lock().unwrap() < 3 {
                    Err::<(), ()>(())
                } else {
                    Ok(42)
                }
            },
            5,
            Duration::from_millis(100),
            Duration::from_secs(2),
            2.0,
        )
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(*attempts.lock().unwrap(), 3);
    }
}