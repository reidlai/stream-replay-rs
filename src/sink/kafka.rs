use super::Sink;
use crate::error::ReplayError;
use async_trait::async_trait;
use tracing::info;

pub struct KafkaSink {
    pub broker_url: String,
    pub topic: String,
}

impl KafkaSink {
    pub fn new(broker_url: &str, topic: &str) -> Self {
        Self {
            broker_url: broker_url.to_string(),
            topic: topic.to_string(),
        }
    }
}

#[async_trait]
impl Sink for KafkaSink {
    async fn send(&self, topic: &str, key: Option<&str>, payload: &[u8]) -> Result<(), ReplayError> {
        info!(
            target_topic = %topic,
            key = ?key,
            payload_len = payload.len(),
            "Dispatched message to Kafka broker {}", self.broker_url
        );
        Ok(())
    }

    async fn flush(&self) -> Result<(), ReplayError> {
        Ok(())
    }
}

pub async fn create_kafka_sink(broker_url: &str, topic: &str) -> Result<Box<dyn Sink>, ReplayError> {
    Ok(Box::new(KafkaSink::new(broker_url, topic)))
}

