pub mod fluvio;
pub mod iceoryx2;
pub mod kafka;
pub mod nats;
pub mod stdout;

use crate::error::ReplayError;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[async_trait]
pub trait Sink: Send + Sync {
    async fn send(&self, topic: &str, key: Option<&str>, payload: &[u8]) -> Result<(), ReplayError>;
    async fn flush(&self) -> Result<(), ReplayError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SinkType {
    Kafka { broker_url: String, topic: String },
    Nats { url: String, topic: String },
    Fluvio { url: String, topic: String },
    Iceoryx2 { service_name: String },
    Stdout { topic: String },
}

pub async fn create_sink(sink_type: &SinkType) -> Result<Box<dyn Sink>, ReplayError> {
    match sink_type {
        SinkType::Stdout { topic } => Ok(Box::new(stdout::StdoutSink::new(topic))),
        SinkType::Kafka { broker_url, topic } => {
            kafka::create_kafka_sink(broker_url, topic).await
        }
        SinkType::Nats { url, topic } => nats::create_nats_sink(url, topic).await,
        SinkType::Fluvio { url, topic } => fluvio::create_fluvio_sink(url, topic).await,
        SinkType::Iceoryx2 { service_name } => {
            iceoryx2::create_iceoryx2_sink(service_name).await
        }
    }
}

