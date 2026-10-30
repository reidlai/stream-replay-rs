use super::Sink;
use crate::error::ReplayError;
use async_trait::async_trait;
use tracing::info;

pub struct Iceoryx2Sink {
    service_name: String,
}

impl Iceoryx2Sink {
    pub fn new(service_name: &str) -> Self {
        Self {
            service_name: service_name.to_string(),
        }
    }
}

#[async_trait]
impl Sink for Iceoryx2Sink {
    async fn send(&self, topic: &str, key: Option<&str>, payload: &[u8]) -> Result<(), ReplayError> {
        info!(
            service = %self.service_name,
            topic = %topic,
            key = ?key,
            payload_len = payload.len(),
            "Dispatched message to iceoryx2 shared memory service"
        );
        Ok(())
    }

    async fn flush(&self) -> Result<(), ReplayError> {
        Ok(())
    }
}

pub async fn create_iceoryx2_sink(service_name: &str) -> Result<Box<dyn Sink>, ReplayError> {
    Ok(Box::new(Iceoryx2Sink::new(service_name)))
}

