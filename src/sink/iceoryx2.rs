use super::{Sink, SinkRecord};
use crate::error::ReplayError;
use async_trait::async_trait;

pub struct Iceoryx2Sink {
    service_name: String,
}

impl Iceoryx2Sink {
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
        }
    }

    pub fn service_name(&self) -> &str {
        &self.service_name
    }
}

#[async_trait]
impl Sink for Iceoryx2Sink {
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

pub async fn create_iceoryx2_sink(service_name: &str) -> Result<Box<dyn Sink>, ReplayError> {
    Ok(Box::new(Iceoryx2Sink::new(service_name)))
}
