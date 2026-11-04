//! Kafka-protocol sink built on the pure-Rust `rskafka` client.
//!
//! Delivery profile (see the README, "Kafka sink"):
//! - `acks = -1`, no idempotence, no compression, no transactions;
//! - one produce request in flight per partition, so order is kept per partition;
//! - the partition is chosen here from the record key (murmur2, the Java default);
//! - the topic must already exist: the replayer never creates topics;
//! - every wait is bounded, and the first delivery failure ends the run with an error.

use super::{Sink, SinkRecord};
use crate::error::ReplayError;
use async_trait::async_trait;
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::ClientBuilder;
use rskafka::record::Record;
use rskafka::BackoffConfig;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{timeout, timeout_at, Instant};
use tracing::{debug, error, info};

const CLIENT_ID: &str = "stream-replay-rs";

/// Tunables of the delivery profile. The defaults are the profile; tests shorten them.
#[derive(Debug, Clone)]
pub struct KafkaOptions {
    /// Wait this long for more records before sending a partial batch.
    pub linger: Duration,
    /// Send a batch once it holds this many bytes.
    pub max_batch_bytes: usize,
    /// Bound for one produce call (`rskafka` 0.6.0 has no request timeout of its own).
    pub request_timeout: Duration,
    /// Bound for retries inside `rskafka` (connection errors, leader changes).
    pub backoff_deadline: Duration,
    /// Bound for the first connection and the topic lookup.
    pub connect_timeout: Duration,
    /// Records queued per partition before `send` waits.
    pub queue_capacity: usize,
}

impl Default for KafkaOptions {
    fn default() -> Self {
        Self {
            linger: Duration::from_millis(5),
            max_batch_bytes: 256 * 1024,
            request_timeout: Duration::from_secs(30),
            backoff_deadline: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(5),
            queue_capacity: 4096,
        }
    }
}

/// Kafka's default partitioner hash (murmur2, seed `0x9747b28c`), compatible with the
/// Java client and with librdkafka's `murmur2_random`.
pub fn murmur2(data: &[u8]) -> i32 {
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;
    let mut h: u32 = 0x9747_b28c ^ (data.len() as u32);

    let mut chunks = data.chunks_exact(4);
    for chunk in &mut chunks {
        let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    let tail = chunks.remainder();
    if tail.len() >= 3 {
        h ^= u32::from(tail[2]) << 16;
    }
    if tail.len() >= 2 {
        h ^= u32::from(tail[1]) << 8;
    }
    if !tail.is_empty() {
        h ^= u32::from(tail[0]);
        h = h.wrapping_mul(M);
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h as i32
}

/// Index of the partition for a key, as Kafka's default partitioner computes it.
pub fn partition_index(key: &[u8], partitions: usize) -> usize {
    ((murmur2(key) & 0x7fff_ffff) as usize) % partitions
}

/// The first delivery failure of a run. Later records are dropped, not sent.
#[derive(Default)]
struct Failure {
    failed: AtomicBool,
    first: Mutex<Option<String>>,
}

impl Failure {
    fn record(&self, message: String) {
        let mut first = self.first.lock().unwrap_or_else(|e| e.into_inner());
        if first.is_none() {
            error!("Kafka delivery failed: {message}");
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

enum Message {
    Record(Record),
    Flush(oneshot::Sender<()>),
}

struct PartitionWorker {
    tx: mpsc::Sender<Message>,
    handle: JoinHandle<()>,
}

pub struct KafkaSink {
    topic: String,
    workers: Vec<PartitionWorker>,
    round_robin: AtomicUsize,
    failure: Arc<Failure>,
    acked: Arc<AtomicU64>,
}

/// Connect, find the topic and start one sender task per partition. Fails if the broker
/// is unreachable or the topic does not exist.
pub async fn connect(
    broker_url: &str,
    topic: &str,
    options: KafkaOptions,
) -> Result<KafkaSink, ReplayError> {
    let addr = broker_url
        .trim_start_matches("kafka://")
        .trim_start_matches("tcp://")
        .to_string();
    info!("Connecting to Kafka-protocol endpoint at {addr}");

    let backoff = BackoffConfig {
        init_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(2),
        base: 3.0,
        deadline: Some(options.backoff_deadline),
    };
    let client = timeout(
        options.connect_timeout,
        ClientBuilder::new(vec![addr.clone()])
            .client_id(CLIENT_ID)
            .backoff_config(backoff)
            .build(),
    )
    .await
    .map_err(|_| ReplayError::Sink(format!("Kafka endpoint {addr} did not answer in time")))?
    .map_err(|e| ReplayError::Sink(format!("cannot connect to Kafka endpoint {addr}: {e}")))?;

    let topics = timeout(options.connect_timeout, client.list_topics())
        .await
        .map_err(|_| ReplayError::Sink(format!("Kafka endpoint {addr} did not list topics")))?
        .map_err(|e| ReplayError::Sink(format!("cannot list topics at {addr}: {e}")))?;
    let partitions: Vec<i32> = topics
        .into_iter()
        .find(|t| t.name == topic)
        .map(|t| t.partitions.into_iter().collect())
        .ok_or_else(|| {
            ReplayError::Sink(format!(
                "topic '{topic}' does not exist at {addr}; the replayer never creates topics"
            ))
        })?;
    if partitions.is_empty() {
        return Err(ReplayError::Sink(format!(
            "topic '{topic}' at {addr} has no partitions"
        )));
    }
    info!("Topic '{topic}' has {} partition(s)", partitions.len());

    let failure = Arc::new(Failure::default());
    let acked = Arc::new(AtomicU64::new(0));
    let mut workers = Vec::with_capacity(partitions.len());
    for partition in partitions {
        let partition_client = timeout(
            options.connect_timeout,
            client.partition_client(topic, partition, UnknownTopicHandling::Error),
        )
        .await
        .map_err(|_| {
            ReplayError::Sink(format!(
                "no leader found for partition {partition} of '{topic}' in time"
            ))
        })?
        .map_err(|e| {
            ReplayError::Sink(format!(
                "partition {partition} of '{topic}' unavailable: {e}"
            ))
        })?;
        let (tx, rx) = mpsc::channel(options.queue_capacity);
        let handle = tokio::spawn(run_partition(
            partition_client,
            rx,
            options.clone(),
            Arc::clone(&failure),
            Arc::clone(&acked),
        ));
        workers.push(PartitionWorker { tx, handle });
    }

    Ok(KafkaSink {
        topic: topic.to_string(),
        workers,
        round_robin: AtomicUsize::new(0),
        failure,
        acked,
    })
}

/// One task per partition keeps a single produce request in flight, which keeps the
/// order of records inside the partition even when a request is retried.
async fn run_partition(
    client: PartitionClient,
    mut rx: mpsc::Receiver<Message>,
    options: KafkaOptions,
    failure: Arc<Failure>,
    acked: Arc<AtomicU64>,
) {
    let mut batch: Vec<Record> = Vec::new();
    let mut bytes = 0usize;
    let mut deadline: Option<Instant> = None;

    loop {
        let message = match deadline {
            None => rx.recv().await,
            Some(at) => match timeout_at(at, rx.recv()).await {
                Ok(message) => message,
                Err(_) => {
                    send_batch(&client, &mut batch, &options, &failure, &acked).await;
                    bytes = 0;
                    deadline = None;
                    continue;
                }
            },
        };
        match message {
            None => {
                send_batch(&client, &mut batch, &options, &failure, &acked).await;
                break;
            }
            Some(Message::Record(record)) => {
                bytes += record.approximate_size();
                batch.push(record);
                if deadline.is_none() {
                    deadline = Some(Instant::now() + options.linger);
                }
                if bytes >= options.max_batch_bytes {
                    send_batch(&client, &mut batch, &options, &failure, &acked).await;
                    bytes = 0;
                    deadline = None;
                }
            }
            Some(Message::Flush(ack)) => {
                send_batch(&client, &mut batch, &options, &failure, &acked).await;
                bytes = 0;
                deadline = None;
                let _ = ack.send(());
            }
        }
    }
}

async fn send_batch(
    client: &PartitionClient,
    batch: &mut Vec<Record>,
    options: &KafkaOptions,
    failure: &Failure,
    acked: &AtomicU64,
) {
    if batch.is_empty() {
        return;
    }
    let records = std::mem::take(batch);
    if failure.is_failed() {
        // After a failure nothing more is sent: the run is already lost.
        return;
    }
    let count = records.len() as u64;
    match timeout(
        options.request_timeout,
        client.produce(records, Compression::NoCompression),
    )
    .await
    {
        Ok(Ok(offsets)) => {
            debug!(
                partition = client.partition(),
                records = count,
                first_offset = ?offsets.first(),
                "Kafka batch acknowledged"
            );
            acked.fetch_add(count, Ordering::SeqCst);
        }
        Ok(Err(e)) => failure.record(format!(
            "produce to partition {} failed: {e}",
            client.partition()
        )),
        Err(_) => failure.record(format!(
            "produce to partition {} timed out after {:?}; {count} record(s) may or may not have been written",
            client.partition(),
            options.request_timeout
        )),
    }
}

impl KafkaSink {
    /// Records the broker has acknowledged so far.
    pub fn acked_records(&self) -> u64 {
        self.acked.load(Ordering::SeqCst)
    }

    pub fn partition_count(&self) -> usize {
        self.workers.len()
    }

    fn fail_if_failed(&self) -> Result<(), ReplayError> {
        match self.failure.message() {
            Some(message) => Err(ReplayError::Sink(message)),
            None => Ok(()),
        }
    }
}

#[async_trait]
impl Sink for KafkaSink {
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
        if topic != self.topic {
            return Err(ReplayError::Sink(format!(
                "this Kafka sink publishes to '{}', not '{topic}'",
                self.topic
            )));
        }
        self.fail_if_failed()?;

        let index = match record.key {
            Some(key) => partition_index(key.as_bytes(), self.workers.len()),
            None => self.round_robin.fetch_add(1, Ordering::Relaxed) % self.workers.len(),
        };
        let headers: BTreeMap<String, Vec<u8>> = record
            .headers
            .iter()
            .map(|(name, value)| (name.clone(), value.clone().into_bytes()))
            .collect();
        let message = Message::Record(Record {
            key: record.key.map(|k| k.as_bytes().to_vec()),
            value: Some(record.payload.to_vec()),
            headers,
            timestamp: record.timestamp,
        });
        self.workers[index]
            .tx
            .send(message)
            .await
            .map_err(|_| ReplayError::Sink("Kafka sender stopped unexpectedly".to_string()))
    }

    async fn flush(&self) -> Result<(), ReplayError> {
        let mut acks = Vec::with_capacity(self.workers.len());
        for worker in &self.workers {
            let (tx, rx) = oneshot::channel();
            if worker.tx.send(Message::Flush(tx)).await.is_ok() {
                acks.push(rx);
            }
        }
        for ack in acks {
            let _ = ack.await;
        }
        self.fail_if_failed()?;
        info!(
            topic = %self.topic,
            acknowledged = self.acked_records(),
            "Flushed all records to Kafka"
        );
        Ok(())
    }
}

impl Drop for KafkaSink {
    fn drop(&mut self) {
        for worker in &self.workers {
            worker.handle.abort();
        }
    }
}

pub async fn create_kafka_sink(
    broker_url: &str,
    topic: &str,
) -> Result<Box<dyn Sink>, ReplayError> {
    Ok(Box::new(
        connect(broker_url, topic, KafkaOptions::default()).await?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Reference values from Kafka's own `UtilsTest.testMurmur2`.
    #[test]
    fn murmur2_matches_kafka_reference_values() {
        let cases: [(&[u8], i32); 6] = [
            (b"21", -973_932_308),
            (b"foobar", -790_332_482),
            (b"a-little-bit-long-string", -985_981_536),
            (b"a-little-bit-longer-string", -1_486_304_829),
            (
                b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8",
                -58_897_971,
            ),
            (b"abc", 479_470_107),
        ];
        for (input, expected) in cases {
            assert_eq!(
                murmur2(input),
                expected,
                "{:?}",
                String::from_utf8_lossy(input)
            );
        }
    }

    #[test]
    fn partition_index_is_stable_and_in_range() {
        for n in [1usize, 2, 3, 12] {
            for key in ["a", "order-1", "order-1_L1", ""] {
                let p = partition_index(key.as_bytes(), n);
                assert!(p < n);
                assert_eq!(p, partition_index(key.as_bytes(), n));
            }
        }
    }
}
