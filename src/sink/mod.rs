#[cfg(feature = "fluvio")]
pub mod fluvio;
#[cfg(feature = "iceoryx2")]
pub mod iceoryx2;
#[cfg(feature = "iggy")]
pub mod iggy;
#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "nats")]
pub mod nats;
pub mod stdout;

use crate::error::ReplayError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// One record on its way to a sink, with the metadata a real broker needs.
#[derive(Debug, Clone)]
pub struct SinkRecord<'a> {
    pub key: Option<&'a str>,
    pub payload: &'a [u8],
    /// Event time of the record after rebasing (what the payload says).
    pub timestamp: DateTime<Utc>,
    /// Header name and value pairs. Names must be unique within one record.
    pub headers: Vec<(String, String)>,
}

#[async_trait]
pub trait Sink: Send + Sync {
    async fn send(&self, topic: &str, key: Option<&str>, payload: &[u8])
        -> Result<(), ReplayError>;

    /// Send a record with headers and an event timestamp. A sink that cannot carry
    /// them falls back to [`Sink::send`].
    async fn send_record(&self, topic: &str, record: &SinkRecord<'_>) -> Result<(), ReplayError> {
        self.send(topic, record.key, record.payload).await
    }

    /// Wait until every record handed to the sink is delivered, and report the first
    /// delivery failure.
    async fn flush(&self) -> Result<(), ReplayError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SinkType {
    #[cfg(feature = "kafka")]
    Kafka {
        broker_url: String,
        topic: String,
    },
    #[cfg(feature = "iggy")]
    Iggy {
        url: String,
        #[serde(alias = "topic")]
        destination: String,
    },
    #[cfg(feature = "nats")]
    Nats {
        url: String,
        #[serde(alias = "topic")]
        subject: String,
    },
    #[cfg(feature = "fluvio")]
    Fluvio {
        #[serde(alias = "url")]
        endpoint: String,
        topic: String,
    },
    #[cfg(feature = "iceoryx2")]
    Iceoryx2 {
        #[serde(alias = "topic")]
        service_name: String,
    },
    Stdout {
        topic: String,
    },
}

impl SinkType {
    pub fn destination(&self) -> &str {
        match self {
            #[cfg(feature = "kafka")]
            SinkType::Kafka { topic, .. } => topic,
            #[cfg(feature = "iggy")]
            SinkType::Iggy { destination, .. } => destination,
            #[cfg(feature = "nats")]
            SinkType::Nats { subject, .. } => subject,
            #[cfg(feature = "fluvio")]
            SinkType::Fluvio { topic, .. } => topic,
            #[cfg(feature = "iceoryx2")]
            SinkType::Iceoryx2 { service_name } => service_name,
            SinkType::Stdout { topic } => topic,
        }
    }
}

pub async fn create_sink(sink_type: &SinkType) -> Result<Box<dyn Sink>, ReplayError> {
    match sink_type {
        SinkType::Stdout { topic } => Ok(Box::new(stdout::StdoutSink::new(topic))),
        #[cfg(feature = "kafka")]
        SinkType::Kafka { broker_url, topic } => kafka::create_kafka_sink(broker_url, topic).await,
        #[cfg(feature = "iggy")]
        SinkType::Iggy { url, destination } => iggy::create_iggy_sink(url, destination).await,
        #[cfg(feature = "nats")]
        SinkType::Nats { url, subject } => nats::create_nats_sink(url, subject).await,
        #[cfg(feature = "fluvio")]
        SinkType::Fluvio { endpoint, topic } => fluvio::create_fluvio_sink(endpoint, topic).await,
        #[cfg(feature = "iceoryx2")]
        SinkType::Iceoryx2 { service_name } => iceoryx2::create_iceoryx2_sink(service_name).await,
    }
}
