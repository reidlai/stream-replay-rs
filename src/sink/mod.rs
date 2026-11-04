pub mod kafka;
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
    Kafka { broker_url: String, topic: String },
    Stdout { topic: String },
}

pub async fn create_sink(sink_type: &SinkType) -> Result<Box<dyn Sink>, ReplayError> {
    match sink_type {
        SinkType::Stdout { topic } => Ok(Box::new(stdout::StdoutSink::new(topic))),
        SinkType::Kafka { broker_url, topic } => kafka::create_kafka_sink(broker_url, topic).await,
    }
}
