//! NATS JetStream sink built on the pure-Rust `async-nats` client.
//!
//! Delivery profile:
//! - connects to a live NATS endpoint with a bounded `connect_timeout` (no simulation);
//! - publishes every record through `async_nats::jetstream::Context` with `HeaderMap` metadata;
//! - embeds replay identity headers (`replay.run_id`, `replay.seq`, `replay.loop`), entity key
//!   (`replay.key`), and event timestamp (`replay.timestamp`);
//! - awaits the JetStream `PubAck` within `request_timeout` and latches on the first failure.

use super::{Sink, SinkRecord};
use crate::error::ReplayError;
use async_nats::header::{HeaderMap, HeaderName, HeaderValue};
use async_nats::jetstream;
use async_trait::async_trait;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::timeout;
use tracing::{debug, error, info};

/// Tunables for the NATS JetStream sink. The defaults are for production; tests shorten them.
#[derive(Debug, Clone)]
pub struct NatsOptions {
    /// Bound for the initial TCP connection and `INFO`/`CONNECT`/`PING`/`PONG` handshake.
    pub connect_timeout: Duration,
    /// Bound for one JetStream publish and its `PubAck` response.
    pub request_timeout: Duration,
}

impl Default for NatsOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
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
    fn record(&self, message: String) {
        let mut first = self.first.lock().unwrap_or_else(|e| e.into_inner());
        if first.is_none() {
            error!("NATS JetStream delivery failed: {message}");
            *first = Some(message);
        }
        self.failed.store(true, Ordering::SeqCst);
    }

    fn is_failed(&self) -> bool {
        self.failed.load(Ordering::SeqCst)
    }

    fn message(&self) -> Option<String> {
        self.first.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

pub struct NatsSink {
    client: async_nats::Client,
    jetstream: jetstream::Context,
    subject: String,
    options: NatsOptions,
    failure: Arc<Failure>,
    acked: Arc<AtomicU64>,
}

impl NatsSink {
    pub fn new(client: async_nats::Client, subject: impl Into<String>) -> Self {
        Self::with_options(client, subject, NatsOptions::default())
    }

    pub fn with_options(
        client: async_nats::Client,
        subject: impl Into<String>,
        options: NatsOptions,
    ) -> Self {
        let mut jetstream = jetstream::new(client.clone());
        // Keep the internal JetStream timeout slightly above `options.request_timeout`
        // so our outer `tokio::time::timeout` produces deterministic timeout diagnostics.
        jetstream.set_timeout(
            options
                .request_timeout
                .saturating_add(Duration::from_secs(5)),
        );
        let subject = subject.into();
        Self {
            client,
            jetstream,
            subject: subject.trim().to_string(),
            options,
            failure: Arc::new(Failure::default()),
            acked: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// Number of records acknowledged by JetStream (`PubAck`) so far.
    pub fn acked_records(&self) -> u64 {
        self.acked.load(Ordering::SeqCst)
    }

    fn fail_if_failed(&self) -> Result<(), ReplayError> {
        if !self.failure.is_failed() {
            return Ok(());
        }
        match self.failure.message() {
            Some(message) => Err(ReplayError::Sink(message)),
            None => Err(ReplayError::Sink("NATS sink failed".to_string())),
        }
    }

    fn record_failure(&self, message: String) -> ReplayError {
        self.failure.record(message.clone());
        ReplayError::Sink(message)
    }
}

/// Build the NATS `HeaderMap` for a `SinkRecord`, preserving all replay headers,
/// optional entity key (`replay.key`), RFC 3339 event timestamp (`replay.timestamp`),
/// and `Nats-Msg-Id` deduplication header when `replay.run_id` and `replay.seq` are present.
fn build_headers(record: &SinkRecord<'_>) -> Result<HeaderMap, String> {
    let mut headers = HeaderMap::new();
    for (name, value) in &record.headers {
        let hdr_name = HeaderName::from_str(name)
            .map_err(|e| format!("invalid NATS header name '{name}': {e}"))?;
        let hdr_val = HeaderValue::from_str(value)
            .map_err(|e| format!("invalid NATS header value for '{name}': {e}"))?;
        headers.insert(hdr_name, hdr_val);
    }
    if headers.get("replay.key").is_none() {
        if let Some(key) = record.key {
            let hdr_val = HeaderValue::from_str(key)
                .map_err(|e| format!("invalid NATS key header value '{key}': {e}"))?;
            headers.insert("replay.key", hdr_val);
        }
    }
    if headers.get("replay.timestamp").is_none() {
        let ts_str = record.timestamp.to_rfc3339();
        let ts_val = HeaderValue::from_str(&ts_str)
            .map_err(|e| format!("invalid NATS timestamp header '{ts_str}': {e}"))?;
        headers.insert("replay.timestamp", ts_val);
    }
    if headers.get("Nats-Msg-Id").is_none() {
        if let (Some(run_id), Some(seq)) = (
            headers.get("replay.run_id").map(|v| v.as_str().to_string()),
            headers.get("replay.seq").map(|v| v.as_str().to_string()),
        ) {
            let msg_id = format!("{run_id}:{seq}");
            let msg_id_val = HeaderValue::from_str(&msg_id)
                .map_err(|e| format!("invalid NATS message id header '{msg_id}': {e}"))?;
            headers.insert("Nats-Msg-Id", msg_id_val);
        }
    }
    Ok(headers)
}

/// Connect to a live NATS endpoint, verify the connection handshake with `flush()`,
/// and return a JetStream-backed [`NatsSink`].
pub async fn connect(
    url: &str,
    subject: &str,
    options: NatsOptions,
) -> Result<NatsSink, ReplayError> {
    let trimmed_url = url.trim();
    if trimmed_url.is_empty() {
        return Err(ReplayError::Sink(
            "NATS endpoint URL must not be empty".to_string(),
        ));
    }
    let trimmed_subject = subject.trim();
    if trimmed_subject.is_empty() {
        return Err(ReplayError::Sink(
            "NATS subject must not be empty".to_string(),
        ));
    }

    info!(
        url = %trimmed_url,
        subject = %trimmed_subject,
        "Connecting to NATS JetStream endpoint"
    );

    let connect_opts = async_nats::ConnectOptions::new()
        .name("stream-replay-rs")
        .connection_timeout(options.connect_timeout)
        .request_timeout(Some(
            options
                .request_timeout
                .saturating_add(Duration::from_secs(5)),
        ))
        .max_reconnects(0);

    let client = timeout(options.connect_timeout, async {
        let c = connect_opts
            .connect(trimmed_url)
            .await
            .map_err(|e| format!("cannot connect to NATS endpoint {trimmed_url}: {e}"))?;
        c.flush()
            .await
            .map_err(|e| format!("NATS handshake with {trimmed_url} failed: {e}"))?;
        Ok::<async_nats::Client, String>(c)
    })
    .await
    .map_err(|_| {
        ReplayError::Sink(format!(
            "NATS endpoint {trimmed_url} did not answer in time ({:?})",
            options.connect_timeout
        ))
    })?
    .map_err(ReplayError::Sink)?;

    Ok(NatsSink::with_options(client, trimmed_subject, options))
}

#[async_trait]
impl Sink for NatsSink {
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

        let target = if topic.is_empty() {
            self.subject.as_str()
        } else {
            topic.trim()
        };
        if target != self.subject {
            return Err(self.record_failure(format!(
                "this NATS sink publishes to '{}', not '{topic}'",
                self.subject
            )));
        }

        let headers = build_headers(record).map_err(|msg| self.record_failure(msg))?;
        let payload = record.payload.to_vec().into();
        let subject = self.subject.clone();

        let publish_and_ack = async {
            let ack_future = self
                .jetstream
                .publish_with_headers(subject, headers, payload)
                .await
                .map_err(|e| format!("publish to NATS subject '{}' failed: {e}", self.subject))?;
            ack_future.await.map_err(|e| {
                let err_str = e.to_string();
                if err_str.to_lowercase().contains("timed out") {
                    format!(
                        "publish to NATS subject '{}' timed out after {:?}; 1 record(s) may or may not have been written",
                        self.subject, self.options.request_timeout
                    )
                } else {
                    format!(
                        "JetStream PubAck for subject '{}' failed (stream may be missing or no responders): {e}",
                        self.subject
                    )
                }
            })
        };

        match timeout(self.options.request_timeout, publish_and_ack).await {
            Ok(Ok(ack)) => {
                debug!(
                    subject = %self.subject,
                    stream = %ack.stream,
                    sequence = ack.sequence,
                    duplicate = ack.duplicate,
                    "NATS JetStream record acknowledged"
                );
                self.acked.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            Ok(Err(err_msg)) => Err(self.record_failure(err_msg)),
            Err(_) => Err(self.record_failure(format!(
                "publish to NATS subject '{}' timed out after {:?}; 1 record(s) may or may not have been written",
                self.subject, self.options.request_timeout
            ))),
        }
    }

    async fn flush(&self) -> Result<(), ReplayError> {
        self.fail_if_failed()?;
        match timeout(self.options.request_timeout, self.client.flush()).await {
            Ok(Ok(())) => {
                info!(
                    subject = %self.subject,
                    acknowledged = self.acked_records(),
                    "Flushed all records to NATS JetStream"
                );
                Ok(())
            }
            Ok(Err(e)) => Err(self.record_failure(format!(
                "NATS flush for subject '{}' failed: {e}",
                self.subject
            ))),
            Err(_) => Err(self.record_failure(format!(
                "NATS flush for subject '{}' timed out after {:?}; records may or may not have been written",
                self.subject, self.options.request_timeout
            ))),
        }
    }
}

pub async fn create_nats_sink(url: &str, subject: &str) -> Result<Box<dyn Sink>, ReplayError> {
    Ok(Box::new(
        connect(url, subject, NatsOptions::default()).await?,
    ))
}
