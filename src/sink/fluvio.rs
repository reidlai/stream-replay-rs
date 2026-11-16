use super::{Sink, SinkRecord};
use crate::error::ReplayError;
use async_trait::async_trait;

pub struct FluvioSink {
    topic: String,
}

impl FluvioSink {
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }
}

#[async_trait]
impl Sink for FluvioSink {
    async fn send(
        &self,
        _topic: &str,
        _key: Option<&str>,
        _payload: &[u8],
    ) -> Result<(), ReplayError> {
        Ok(())
    }

    async fn send_record(&self, topic: &str, record: &SinkRecord<'_>) -> Result<(), ReplayError> {
        self.send(topic, record.key, record.payload).await
    }

    async fn flush(&self) -> Result<(), ReplayError> {
        Ok(())
    }
}

pub async fn create_fluvio_sink(
    _endpoint: &str,
    topic: &str,
) -> Result<Box<dyn Sink>, ReplayError> {
    Ok(Box::new(FluvioSink::new(topic)))
}
