use domain_types::audit::AuditEvent;
use reqwest::Client;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;

const MAX_RING_BUFFER: usize = 1000;

#[derive(Debug, thiserror::Error)]
pub enum EmitterError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Service returned error: {0}")]
    Service(u16),
}

pub struct AuditEmitter {
    client: Client,
    events_url: String,
    auth_token: String,
    ring_buffer: Arc<Mutex<VecDeque<AuditEvent>>>,
}

impl AuditEmitter {
    pub fn new(events_url: impl Into<String>, auth_token: impl Into<String>) -> Self {
        Self {
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .expect("Failed to create HTTP client"),
            events_url: events_url.into(),
            auth_token: auth_token.into(),
            ring_buffer: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_RING_BUFFER))),
        }
    }

    pub fn ring_buffer(&self) -> Arc<Mutex<VecDeque<AuditEvent>>> {
        self.ring_buffer.clone()
    }

    pub async fn emit(&self, event: AuditEvent) -> Result<(), EmitterError> {
        for attempt in 0..3 {
            match self.try_emit(&event).await {
                Ok(()) => return Ok(()),
                Err(_e) if attempt < 2 => {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        100 * 2u64.pow(attempt),
                    ))
                    .await;
                    continue;
                }
                Err(e) => {
                    self.buffer_event(event).await;
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    async fn try_emit(&self, event: &AuditEvent) -> Result<(), EmitterError> {
        let resp = self
            .client
            .post(&self.events_url)
            .bearer_auth(&self.auth_token)
            .json(event)
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(EmitterError::Service(resp.status().as_u16()));
        }

        Ok(())
    }

    async fn buffer_event(&self, event: AuditEvent) {
        let mut buffer = self.ring_buffer.lock().await;
        if buffer.len() >= MAX_RING_BUFFER {
            buffer.pop_front();
        }
        buffer.push_back(event);
    }

    pub async fn flush_buffer(&self) -> usize {
        let mut buffer = self.ring_buffer.lock().await;
        let count = buffer.len();
        buffer.clear();
        count
    }
}

pub fn create_idempotency_key(event_type: &str, user_uuid: &uuid::Uuid) -> String {
    let ts = chrono::Utc::now().timestamp();
    format!("{}-{}-{}", event_type, user_uuid, ts)
}
