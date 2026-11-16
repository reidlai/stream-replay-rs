use super::{Sink, SinkRecord};
use crate::error::ReplayError;
use async_trait::async_trait;

pub struct NatsSink {
    client: async_nats::Client,
    subject: String,
}

impl NatsSink {
    pub fn new(client: async_nats::Client, subject: impl Into<String>) -> Self {
        Self {
            client,
            subject: subject.into(),
        }
    }

    pub fn subject(&self) -> &str {
        &self.subject
    }
}

#[async_trait]
impl Sink for NatsSink {
    async fn send(
        &self,
        topic: &str,
        _key: Option<&str>,
        payload: &[u8],
    ) -> Result<(), ReplayError> {
        let target = if topic.is_empty() {
            &self.subject
        } else {
            topic
        };
        self.client
            .publish(target.to_string(), payload.to_vec().into())
            .await
            .map_err(|e| ReplayError::Sink(format!("NATS publish failed: {e}")))?;
        Ok(())
    }

    async fn send_record(&self, topic: &str, record: &SinkRecord<'_>) -> Result<(), ReplayError> {
        self.send(topic, record.key, record.payload).await
    }

    async fn flush(&self) -> Result<(), ReplayError> {
        self.client
            .flush()
            .await
            .map_err(|e| ReplayError::Sink(format!("NATS flush failed: {e}")))?;
        Ok(())
    }
}

pub async fn create_nats_sink(url: &str, subject: &str) -> Result<Box<dyn Sink>, ReplayError> {
    let client = async_nats::connect(url)
        .await
        .map_err(|e| ReplayError::Sink(format!("Failed to connect to NATS at {url}: {e}")))?;
    Ok(Box::new(NatsSink::new(client, subject)))
}
