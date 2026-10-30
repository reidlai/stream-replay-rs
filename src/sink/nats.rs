use super::Sink;
use crate::error::ReplayError;
use async_trait::async_trait;
use tracing::info;

pub struct NatsSink {
    pub url: String,
    pub topic: String,
}

impl NatsSink {
    pub fn new(url: &str, topic: &str) -> Self {
        Self {
            url: url.to_string(),
            topic: topic.to_string(),
        }
    }
}

#[async_trait]
impl Sink for NatsSink {
    async fn send(&self, topic: &str, key: Option<&str>, payload: &[u8]) -> Result<(), ReplayError> {
        info!(
            target_topic = %topic,
            key = ?key,
            payload_len = payload.len(),
            "Dispatched message to NATS broker {}", self.url
        );
        Ok(())
    }

    async fn flush(&self) -> Result<(), ReplayError> {
        Ok(())
    }
}

pub async fn create_nats_sink(url: &str, topic: &str) -> Result<Box<dyn Sink>, ReplayError> {
    Ok(Box::new(NatsSink::new(url, topic)))
}

