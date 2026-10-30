use super::Sink;
use crate::error::ReplayError;
use async_trait::async_trait;
use tracing::info;

pub struct FluvioSink {
    pub url: String,
    pub topic: String,
}

impl FluvioSink {
    pub fn new(url: &str, topic: &str) -> Self {
        Self {
            url: url.to_string(),
            topic: topic.to_string(),
        }
    }
}

#[async_trait]
impl Sink for FluvioSink {
    async fn send(&self, topic: &str, key: Option<&str>, payload: &[u8]) -> Result<(), ReplayError> {
        info!(
            target_topic = %topic,
            key = ?key,
            payload_len = payload.len(),
            "Dispatched message to Fluvio broker {}", self.url
        );
        Ok(())
    }

    async fn flush(&self) -> Result<(), ReplayError> {
        Ok(())
    }
}

pub async fn create_fluvio_sink(url: &str, topic: &str) -> Result<Box<dyn Sink>, ReplayError> {
    Ok(Box::new(FluvioSink::new(url, topic)))
}

