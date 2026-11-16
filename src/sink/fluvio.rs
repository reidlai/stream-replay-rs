//! Fluvio distributed streaming sink built on the pure-Rust `fluvio` client.
//!
//! Delivery profile:
//! - connects to a live Fluvio cluster (`Fluvio::connect_with_config` or `Fluvio::connect`)
//!   within a bounded `connect_timeout` (no simulation);
//! - initializes a `TopicProducerPool` configured with `DeliverySemantic::AtLeastOnce`,
//!   `Isolation::ReadCommitted`, `linger`, `batch_size`, and `timeout`;
//! - routes records by entity key (`RecordKey::from(key)` or `RecordKey::NULL`) and embeds
//!   replay tracking headers (`replay.run_id`, `replay.seq`, `replay.loop`, `replay.key`,
//!   `replay.timestamp`);
//! - flushes and awaits broker delivery acknowledgment (`ProduceOutput::wait()`) within
//!   `request_timeout`, latching the first failure so subsequent calls fail immediately.

use super::{Sink, SinkRecord};
use crate::error::ReplayError;
use async_trait::async_trait;
use fluvio::{Fluvio, FluvioClusterConfig, TopicProducerConfigBuilder, TopicProducerPool};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::timeout;
use tracing::{debug, error, info};

pub use fluvio::{DeliverySemantic, Isolation, RecordKey, RetryPolicy, RetryStrategy};

/// Tunables for the Fluvio distributed streaming sink. The defaults are for production; tests shorten them.
#[derive(Debug, Clone)]
pub struct FluvioOptions {
    /// Bound for the initial SC/SPU connection and topic metadata lookup.
    pub connect_timeout: Duration,
    /// Bound for one produce + flush + SPU acknowledgment cycle.
    pub request_timeout: Duration,
    /// Time to wait for additional records before sending a batch.
    pub linger: Duration,
    /// Maximum batch size in bytes accumulated before sending to the SPU.
    pub batch_size: usize,
    /// Whether to use the SPU's local address when advertised in cluster metadata.
    pub use_spu_local_address: bool,
}

impl Default for FluvioOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            linger: Duration::from_millis(5),
            batch_size: 256 * 1024,
            use_spu_local_address: false,
        }
    }
}

/// Latches the first delivery failure of a run so subsequent sends fail immediately.
#[derive(Default)]
struct Failure {
    failed: AtomicBool,
    first: Mutex<Option<String>>,
}

impl Failure {
    fn record(&self, message: String) -> String {
        let mut first = self.first.lock().unwrap_or_else(|e| e.into_inner());
        let latched = match first.as_ref() {
            Some(existing) => existing.clone(),
            None => {
                error!("Fluvio delivery failed: {message}");
                *first = Some(message.clone());
                message
            }
        };
        self.failed.store(true, Ordering::SeqCst);
        latched
    }

    fn is_failed(&self) -> bool {
        self.failed.load(Ordering::SeqCst)
    }

    fn message(&self) -> Option<String> {
        self.first.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

pub struct FluvioSink {
    _client: Option<Fluvio>,
    producer: TopicProducerPool,
    endpoint: String,
    topic: String,
    options: FluvioOptions,
    failure: Arc<Failure>,
    acked: Arc<AtomicU64>,
}

impl FluvioSink {
    pub fn new(producer: TopicProducerPool, topic: impl Into<String>) -> Self {
        Self::with_options(producer, topic, FluvioOptions::default())
    }

    pub fn with_options(
        producer: TopicProducerPool,
        topic: impl Into<String>,
        options: FluvioOptions,
    ) -> Self {
        let topic = topic.into();
        Self {
            _client: None,
            producer,
            endpoint: String::new(),
            topic: topic.trim().to_string(),
            options,
            failure: Arc::new(Failure::default()),
            acked: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn options(&self) -> &FluvioOptions {
        &self.options
    }

    /// Number of records acknowledged by the Fluvio SPU so far.
    pub fn acked_records(&self) -> u64 {
        self.acked.load(Ordering::SeqCst)
    }

    fn fail_if_failed(&self) -> Result<(), ReplayError> {
        if !self.failure.is_failed() {
            return Ok(());
        }
        match self.failure.message() {
            Some(message) => Err(ReplayError::Sink(message)),
            None => Err(ReplayError::Sink("Fluvio sink failed".to_string())),
        }
    }

    fn record_failure(&self, message: String) -> ReplayError {
        ReplayError::Sink(self.failure.record(message))
    }
}

fn validate_header_token(label: &str, value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err(format!("invalid Fluvio {label}: must not be empty"));
    }
    if value.contains(['\r', '\n', '\0']) {
        return Err(format!(
            "invalid Fluvio {label} '{value}': control characters are not allowed"
        ));
    }
    Ok(())
}

/// Build the replay metadata header map for a [`SinkRecord`], preserving all record headers,
/// optional entity key (`replay.key`), and RFC 3339 event timestamp (`replay.timestamp`).
pub fn build_headers(record: &SinkRecord<'_>) -> Result<BTreeMap<String, String>, String> {
    let mut headers = BTreeMap::new();
    for (name, value) in &record.headers {
        let trimmed_name = name.trim();
        validate_header_token("header name", trimmed_name)?;
        if value.contains(['\r', '\n', '\0']) {
            return Err(format!(
                "invalid Fluvio header value for '{trimmed_name}': control characters are not allowed"
            ));
        }
        headers.insert(trimmed_name.to_string(), value.clone());
    }
    if !headers.contains_key("replay.key") {
        if let Some(key) = record.key {
            if key.contains(['\r', '\n', '\0']) {
                return Err(format!(
                    "invalid Fluvio key header value '{key}': control characters are not allowed"
                ));
            }
            headers.insert("replay.key".to_string(), key.to_string());
        }
    }
    if !headers.contains_key("replay.timestamp") {
        headers.insert(
            "replay.timestamp".to_string(),
            record.timestamp.to_rfc3339(),
        );
    }
    Ok(headers)
}

/// Embed replay tracking headers into a JSON object payload when headers are present so
/// downstream Fluvio consumers receive both the payload fields and replay lineage metadata.
pub fn encode_wire_payload(payload: &[u8], headers: &BTreeMap<String, String>) -> Vec<u8> {
    if headers.is_empty() {
        return payload.to_vec();
    }
    if let Ok(serde_json::Value::Object(mut obj)) =
        serde_json::from_slice::<serde_json::Value>(payload)
    {
        for (k, v) in headers {
            obj.entry(k.clone())
                .or_insert_with(|| serde_json::Value::String(v.clone()));
        }
        if let Ok(encoded) = serde_json::to_vec(&obj) {
            return encoded;
        }
    }
    payload.to_vec()
}

fn normalize_endpoint(endpoint: &str) -> Result<String, ReplayError> {
    let trimmed = endpoint.trim();
    if trimmed.is_empty() {
        return Err(ReplayError::Sink(
            "Fluvio endpoint must not be empty".to_string(),
        ));
    }
    let stripped = trimmed
        .strip_prefix("fluvio://")
        .or_else(|| trimmed.strip_prefix("tcp://"))
        .unwrap_or(trimmed)
        .trim();
    if stripped.is_empty() {
        return Err(ReplayError::Sink(
            "Fluvio endpoint must not be empty".to_string(),
        ));
    }
    Ok(stripped.to_string())
}

/// Connect to a live Fluvio cluster, verify topic metadata, and create a partitioned
/// [`FluvioSink`] backed by [`TopicProducerPool`].
pub async fn connect(
    endpoint: &str,
    topic: &str,
    options: FluvioOptions,
) -> Result<FluvioSink, ReplayError> {
    let addr = normalize_endpoint(endpoint)?;
    let trimmed_topic = topic.trim();
    if trimmed_topic.is_empty() {
        return Err(ReplayError::Sink(
            "Fluvio topic must not be empty".to_string(),
        ));
    }
    if options.batch_size == 0 {
        return Err(ReplayError::Sink(
            "Fluvio batch_size must be at least 1 byte".to_string(),
        ));
    }

    info!(
        endpoint = %addr,
        topic = %trimmed_topic,
        linger = ?options.linger,
        batch_size = options.batch_size,
        "Connecting to Fluvio cluster endpoint"
    );

    // Keep the internal producer retry timeout slightly above `options.request_timeout`
    // so our outer `tokio::time::timeout` produces deterministic timeout diagnostics.
    let internal_timeout = options
        .request_timeout
        .saturating_add(Duration::from_secs(5));
    let retry_policy = RetryPolicy {
        max_retries: 2,
        initial_delay: Duration::from_millis(25),
        max_delay: Duration::from_millis(250),
        timeout: internal_timeout,
        strategy: RetryStrategy::ExponentialBackoff,
    };

    let producer_config = TopicProducerConfigBuilder::default()
        .linger(options.linger)
        .batch_size(options.batch_size)
        .timeout(internal_timeout)
        .isolation(Isolation::ReadCommitted)
        .delivery_semantic(DeliverySemantic::AtLeastOnce(retry_policy))
        .build()
        .map_err(|e| ReplayError::Sink(format!("invalid Fluvio producer configuration: {e}")))?;

    let addr_for_connect = addr.clone();
    let topic_for_connect = trimmed_topic.to_string();
    let use_spu_local_address = options.use_spu_local_address;

    let (client, producer) = timeout(options.connect_timeout, async move {
        let client = if addr_for_connect.eq_ignore_ascii_case("profile")
            || addr_for_connect.eq_ignore_ascii_case("default")
        {
            Fluvio::connect()
                .await
                .map_err(|e| format!("cannot connect to Fluvio default profile: {e}"))?
        } else if let Some(profile_name) = addr_for_connect.strip_prefix("profile:") {
            let profile_name = profile_name.trim();
            if profile_name.is_empty() {
                return Err("Fluvio profile name must not be empty".to_string());
            }
            Fluvio::connect_with_profile(profile_name)
                .await
                .map_err(|e| format!("cannot connect to Fluvio profile '{profile_name}': {e}"))?
        } else {
            let mut cluster_config = FluvioClusterConfig::new(addr_for_connect.clone());
            cluster_config.use_spu_local_address = use_spu_local_address;
            cluster_config.client_id = Some("stream-replay-rs".to_string());
            Fluvio::connect_with_config(&cluster_config)
                .await
                .map_err(|e| format!("cannot connect to Fluvio endpoint {addr_for_connect}: {e}"))?
        };

        let producer = client
            .topic_producer_with_config(&topic_for_connect, producer_config)
            .await
            .map_err(|e| {
                format!(
                    "cannot initialize Fluvio topic producer for '{topic_for_connect}' at {addr_for_connect}: {e}"
                )
            })?;
        Ok::<(Fluvio, TopicProducerPool), String>((client, producer))
    })
    .await
    .map_err(|_| {
        ReplayError::Sink(format!(
            "Fluvio endpoint {addr} did not answer in time ({:?}; timed out)",
            options.connect_timeout
        ))
    })?
    .map_err(ReplayError::Sink)?;

    Ok(FluvioSink {
        _client: Some(client),
        producer,
        endpoint: addr,
        topic: trimmed_topic.to_string(),
        options,
        failure: Arc::new(Failure::default()),
        acked: Arc::new(AtomicU64::new(0)),
    })
}

#[async_trait]
impl Sink for FluvioSink {
    async fn send(
        &self,
        topic: &str,
        key: Option<&str>,
        payload: &[u8],
    ) -> Result<(), ReplayError> {
        let record = SinkRecord {
            key,
            payload,
            timestamp: chrono::Utc::now(),
            headers: Vec::new(),
        };
        self.send_record(topic, &record).await
    }

    async fn send_record(&self, topic: &str, record: &SinkRecord<'_>) -> Result<(), ReplayError> {
        self.fail_if_failed()?;

        let trimmed_topic = topic.trim();
        let target = if trimmed_topic.is_empty() {
            self.topic.as_str()
        } else {
            trimmed_topic
        };
        if target != self.topic {
            return Err(self.record_failure(format!(
                "this Fluvio sink publishes to '{}', not '{topic}'",
                self.topic
            )));
        }

        let headers = build_headers(record).map_err(|msg| self.record_failure(msg))?;
        let wire_payload = encode_wire_payload(record.payload, &headers);
        let record_key = match record.key {
            Some(k) => RecordKey::from(k),
            None => RecordKey::NULL,
        };

        let produce_and_ack = async {
            let output = self
                .producer
                .send(record_key, wire_payload)
                .await
                .map_err(|e| format!("produce to Fluvio topic '{}' failed: {e}", self.topic))?;
            let flush_res = self.producer.flush().await;
            match output.wait().await {
                Ok(meta) => {
                    if let Err(e) = flush_res {
                        return Err(format!(
                            "flush to Fluvio topic '{}' failed: {e}",
                            self.topic
                        ));
                    }
                    Ok(meta)
                }
                Err(wait_err) => {
                    let combined = match &flush_res {
                        Err(flush_err) => format!("{wait_err} ({flush_err})"),
                        Ok(()) => wait_err.to_string(),
                    };
                    let lower = combined.to_lowercase();
                    if lower.contains("timed out") || lower.contains("timeout") {
                        Err(format!(
                            "produce to Fluvio topic '{}' timed out after {:?}; 1 record(s) may or may not have been written: {combined}",
                            self.topic, self.options.request_timeout
                        ))
                    } else {
                        Err(format!(
                            "Fluvio SPU delivery acknowledgment for topic '{}' failed: {combined}",
                            self.topic
                        ))
                    }
                }
            }
        };

        match timeout(self.options.request_timeout, produce_and_ack).await {
            Ok(Ok(metadata)) => {
                debug!(
                    topic = %self.topic,
                    partition = metadata.partition_id(),
                    offset = metadata.offset(),
                    "Fluvio record acknowledged by SPU"
                );
                self.acked.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            Ok(Err(err_msg)) => Err(self.record_failure(err_msg)),
            Err(_) => Err(self.record_failure(format!(
                "produce to Fluvio topic '{}' timed out after {:?}; 1 record(s) may or may not have been written",
                self.topic, self.options.request_timeout
            ))),
        }
    }

    async fn flush(&self) -> Result<(), ReplayError> {
        self.fail_if_failed()?;
        match timeout(self.options.request_timeout, self.producer.flush()).await {
            Ok(Ok(())) => {
                info!(
                    topic = %self.topic,
                    acknowledged = self.acked_records(),
                    "Flushed all records to Fluvio"
                );
                Ok(())
            }
            Ok(Err(e)) => Err(self.record_failure(format!(
                "Fluvio flush for topic '{}' failed: {e}",
                self.topic
            ))),
            Err(_) => Err(self.record_failure(format!(
                "Fluvio flush for topic '{}' timed out after {:?}; records may or may not have been written",
                self.topic, self.options.request_timeout
            ))),
        }
    }
}

pub async fn create_fluvio_sink(endpoint: &str, topic: &str) -> Result<Box<dyn Sink>, ReplayError> {
    Ok(Box::new(
        connect(endpoint, topic, FluvioOptions::default()).await?,
    ))
}
