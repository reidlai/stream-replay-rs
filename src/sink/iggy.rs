use crate::error::ReplayError;
use crate::sink::{Sink, SinkRecord};
use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::time::{sleep_until, Instant};
use tracing::{debug, error, info, warn};

pub const DEFAULT_IGGY_USERNAME: &str = "iggy";
pub const DEFAULT_IGGY_PASSWORD: &str = "iggy";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_LINGER: Duration = Duration::from_millis(5);
const DEFAULT_MAX_BATCH_MESSAGES: usize = 1000;
const DEFAULT_MAX_BATCH_BYTES: usize = 512 * 1024;
const DEFAULT_QUEUE_CAPACITY: usize = 8192;

/// Normalize a user-supplied Iggy HTTP endpoint into a base URL without a trailing slash.
pub fn normalize_iggy_url(raw: &str) -> String {
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    }
}

/// Split an Iggy destination into `(stream, topic)`.
///
/// Accepts `stream/topic` (e.g. `orders/intake`). When no `/` is present, defaults the
/// stream name to `"default"` and uses the entire destination as the topic name.
pub fn parse_stream_topic(destination: &str) -> Result<(String, String), ReplayError> {
    let trimmed = destination.trim();
    if trimmed.is_empty() {
        return Err(ReplayError::Config(
            "Iggy destination must not be empty (expected '<stream>/<topic>' or '<topic>')".into(),
        ));
    }
    if let Some((stream, topic)) = trimmed.split_once('/') {
        let stream = stream.trim();
        let topic = topic.trim();
        if stream.is_empty() || topic.is_empty() {
            return Err(ReplayError::Config(format!(
                "Invalid Iggy destination '{destination}': expected '<stream>/<topic>'"
            )));
        }
        Ok((stream.to_string(), topic.to_string()))
    } else {
        Ok(("default".to_string(), trimmed.to_string()))
    }
}

/// Deterministic FNV-1a 64-bit partition hash so keyed records preserve per-key ordering.
fn key_partition(key: &str, partitions: usize) -> usize {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in key.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash as usize) % partitions
}

#[derive(Serialize)]
struct LoginRequest<'a> {
    username: &'a str,
    password: &'a str,
}

#[derive(Deserialize)]
struct LoginResponse {
    access_token: AccessToken,
}

#[derive(Deserialize)]
struct AccessToken {
    token: String,
}

#[derive(Deserialize)]
struct TopicInfo {
    #[serde(default)]
    partitions_count: u32,
}

#[derive(Serialize)]
struct CreateStreamRequest<'a> {
    name: &'a str,
}

#[derive(Serialize)]
struct CreateTopicRequest<'a> {
    name: &'a str,
    partitions_count: u32,
}

#[derive(Serialize)]
struct SendMessagesRequest<'a> {
    partitioning: Partitioning<'a>,
    messages: &'a [IggyMessage],
}

#[derive(Serialize)]
struct Partitioning<'a> {
    kind: &'a str,
    value: &'a str,
}

#[derive(Serialize)]
struct IggyMessage {
    id: u128,
    payload: String,
}

#[derive(Default)]
struct Failure {
    failed: AtomicBool,
    first: Mutex<Option<String>>,
}

impl Failure {
    fn record(&self, msg: String) {
        let mut first = self.first.lock().unwrap_or_else(|e| e.into_inner());
        if first.is_none() {
            error!("Apache Iggy delivery failed: {msg}");
            *first = Some(msg);
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

struct QueuedRecord {
    payload_b64: String,
    approx_bytes: usize,
}

enum WorkerCommand {
    Record(QueuedRecord),
    Flush(oneshot::Sender<()>),
}

struct SharedAuth {
    client: Client,
    base_url: String,
    username: String,
    password: String,
    token: RwLock<String>,
}

impl SharedAuth {
    async fn login(
        client: &Client,
        base_url: &str,
        username: &str,
        password: &str,
    ) -> Result<String, ReplayError> {
        let endpoint = format!("{base_url}/users/login");
        let resp = client
            .post(&endpoint)
            .json(&LoginRequest { username, password })
            .send()
            .await
            .map_err(|e| {
                ReplayError::Sink(format!("Failed to reach Apache Iggy at {base_url}: {e}"))
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ReplayError::Sink(format!(
                "Apache Iggy login failed ({status}): {body}"
            )));
        }

        let parsed: LoginResponse = resp
            .json()
            .await
            .map_err(|e| ReplayError::Sink(format!("Invalid Iggy login response: {e}")))?;
        Ok(parsed.access_token.token)
    }

    async fn refresh(&self) -> Result<String, ReplayError> {
        let new_token =
            Self::login(&self.client, &self.base_url, &self.username, &self.password).await?;
        *self.token.write().await = new_token.clone();
        Ok(new_token)
    }
}

/// High-throughput batched native Apache Iggy HTTP sink.
///
/// Spawns one background batching worker per topic partition, accumulating up to
/// `DEFAULT_MAX_BATCH_MESSAGES` (1,000) or `DEFAULT_MAX_BATCH_BYTES` (512 KiB) per
/// HTTP keep-alive POST while preserving per-key ordering.
pub struct IggySink {
    stream: String,
    topic: String,
    workers: Vec<mpsc::Sender<WorkerCommand>>,
    round_robin: AtomicUsize,
    failure: Arc<Failure>,
}

impl IggySink {
    pub async fn connect(
        url: &str,
        destination: &str,
        username: String,
        password: String,
    ) -> Result<Self, ReplayError> {
        let base_url = normalize_iggy_url(url);
        let (stream, topic) = parse_stream_topic(destination)?;

        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .pool_max_idle_per_host(16)
            .build()
            .map_err(|e| ReplayError::Sink(format!("Failed to build Iggy HTTP client: {e}")))?;

        info!(
            "Connecting to Apache Iggy HTTP endpoint at {} (destination: {}/{})",
            base_url, stream, topic
        );

        let token = SharedAuth::login(&client, &base_url, &username, &password).await?;
        let partitions_count =
            Self::ensure_stream_and_topic(&client, &base_url, &token, &stream, &topic).await?;

        let auth = Arc::new(SharedAuth {
            client,
            base_url: base_url.clone(),
            username,
            password,
            token: RwLock::new(token),
        });
        let failure = Arc::new(Failure::default());
        let messages_url = format!("{base_url}/streams/{stream}/topics/{topic}/messages");

        let worker_count = partitions_count.max(1) as usize;
        let mut workers = Vec::with_capacity(worker_count);
        for partition_id in 0..worker_count {
            let (tx, rx) = mpsc::channel(DEFAULT_QUEUE_CAPACITY);
            let partition_value = BASE64.encode((partition_id as u32).to_le_bytes());
            let worker_auth = auth.clone();
            let worker_failure = failure.clone();
            let worker_url = messages_url.clone();
            tokio::spawn(async move {
                run_partition_worker(
                    rx,
                    worker_auth,
                    worker_url,
                    partition_value,
                    worker_failure,
                    DEFAULT_LINGER,
                    DEFAULT_MAX_BATCH_MESSAGES,
                    DEFAULT_MAX_BATCH_BYTES,
                )
                .await;
            });
            workers.push(tx);
        }

        info!(
            "Apache Iggy sink ready for {}/{} ({} batched partition workers)",
            stream, topic, worker_count
        );

        Ok(Self {
            stream,
            topic,
            workers,
            round_robin: AtomicUsize::new(0),
            failure,
        })
    }

    async fn ensure_stream_and_topic(
        client: &Client,
        base_url: &str,
        token: &str,
        stream: &str,
        topic: &str,
    ) -> Result<u32, ReplayError> {
        let topic_url = format!("{base_url}/streams/{stream}/topics/{topic}");
        let check = client
            .get(&topic_url)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|e| {
                ReplayError::Sink(format!("Failed to verify Iggy topic {stream}/{topic}: {e}"))
            })?;

        if check.status().is_success() {
            let info = check.json::<TopicInfo>().await.unwrap_or(TopicInfo {
                partitions_count: 1,
            });
            let count = info.partitions_count.max(1);
            debug!("Verified existing Iggy destination {stream}/{topic} ({count} partitions)");
            return Ok(count);
        }

        // Provision stream idempotently (400 stream_name_already_exists is fine)
        let stream_resp = client
            .post(format!("{base_url}/streams"))
            .bearer_auth(token)
            .json(&CreateStreamRequest { name: stream })
            .send()
            .await
            .map_err(|e| ReplayError::Sink(format!("Failed to create Iggy stream: {e}")))?;
        if !stream_resp.status().is_success() && stream_resp.status() != StatusCode::BAD_REQUEST {
            let status = stream_resp.status();
            let body = stream_resp.text().await.unwrap_or_default();
            warn!("Unexpected status creating Iggy stream '{stream}': {status} {body}");
        }

        // Provision topic idempotently
        let topic_resp = client
            .post(format!("{base_url}/streams/{stream}/topics"))
            .bearer_auth(token)
            .json(&CreateTopicRequest {
                name: topic,
                partitions_count: 1,
            })
            .send()
            .await
            .map_err(|e| ReplayError::Sink(format!("Failed to create Iggy topic: {e}")))?;
        if !topic_resp.status().is_success() && topic_resp.status() != StatusCode::BAD_REQUEST {
            let status = topic_resp.status();
            let body = topic_resp.text().await.unwrap_or_default();
            return Err(ReplayError::Sink(format!(
                "Failed to provision Iggy topic {stream}/{topic} ({status}): {body}"
            )));
        }

        info!("Provisioned Apache Iggy destination {stream}/{topic} (1 partition)");
        Ok(1)
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_partition_worker(
    mut rx: mpsc::Receiver<WorkerCommand>,
    auth: Arc<SharedAuth>,
    messages_url: String,
    partition_value: String,
    failure: Arc<Failure>,
    linger: Duration,
    max_batch_messages: usize,
    max_batch_bytes: usize,
) {
    let mut batch: Vec<IggyMessage> = Vec::with_capacity(max_batch_messages);
    let mut batch_bytes: usize = 0;

    while let Some(cmd) = rx.recv().await {
        let mut pending_flushes: Vec<oneshot::Sender<()>> = Vec::new();
        match cmd {
            WorkerCommand::Record(rec) => {
                batch_bytes += rec.approx_bytes;
                batch.push(IggyMessage {
                    id: 0,
                    payload: rec.payload_b64,
                });
            }
            WorkerCommand::Flush(tx) => {
                pending_flushes.push(tx);
            }
        }

        if !batch.is_empty() && pending_flushes.is_empty() {
            let deadline = Instant::now() + linger;
            while batch.len() < max_batch_messages && batch_bytes < max_batch_bytes {
                // Fast-path non-blocking drain first when producer is at max speed
                match rx.try_recv() {
                    Ok(WorkerCommand::Record(rec)) => {
                        batch_bytes += rec.approx_bytes;
                        batch.push(IggyMessage {
                            id: 0,
                            payload: rec.payload_b64,
                        });
                        continue;
                    }
                    Ok(WorkerCommand::Flush(tx)) => {
                        pending_flushes.push(tx);
                        break;
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => break,
                    Err(mpsc::error::TryRecvError::Empty) => {}
                }

                // Wait up to linger deadline for more records
                tokio::select! {
                    biased;
                    maybe_cmd = rx.recv() => match maybe_cmd {
                        Some(WorkerCommand::Record(rec)) => {
                            batch_bytes += rec.approx_bytes;
                            batch.push(IggyMessage {
                                id: 0,
                                payload: rec.payload_b64,
                            });
                        }
                        Some(WorkerCommand::Flush(tx)) => {
                            pending_flushes.push(tx);
                            break;
                        }
                        None => break,
                    },
                    _ = sleep_until(deadline) => break,
                }
            }
        }

        if !batch.is_empty() && !failure.is_failed() {
            if let Err(err) =
                send_batch(&auth, &messages_url, &partition_value, &batch).await
            {
                failure.record(err.to_string());
            }
        }
        batch.clear();
        batch_bytes = 0;

        for tx in pending_flushes {
            let _ = tx.send(());
        }
    }
}

async fn send_batch(
    auth: &SharedAuth,
    messages_url: &str,
    partition_value: &str,
    messages: &[IggyMessage],
) -> Result<(), ReplayError> {
    let body = SendMessagesRequest {
        partitioning: Partitioning {
            kind: "partition_id",
            value: partition_value,
        },
        messages,
    };

    let token = auth.token.read().await.clone();
    let resp = auth
        .client
        .post(messages_url)
        .bearer_auth(&token)
        .json(&body)
        .send()
        .await
        .map_err(|e| ReplayError::Sink(format!("Failed to send batch to Iggy: {e}")))?;

    if resp.status().is_success() {
        return Ok(());
    }

    if resp.status() == StatusCode::UNAUTHORIZED {
        let refreshed = auth.refresh().await?;
        let retry = auth
            .client
            .post(messages_url)
            .bearer_auth(&refreshed)
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                ReplayError::Sink(format!("Failed to resend batch to Iggy after refresh: {e}"))
            })?;
        if retry.status().is_success() {
            return Ok(());
        }
        let status = retry.status();
        let err_body = retry.text().await.unwrap_or_default();
        return Err(ReplayError::Sink(format!(
            "Apache Iggy rejected batch after token refresh ({status}): {err_body}"
        )));
    }

    let status = resp.status();
    let err_body = resp.text().await.unwrap_or_default();
    Err(ReplayError::Sink(format!(
        "Apache Iggy rejected batch ({status}): {err_body}"
    )))
}

#[async_trait]
impl Sink for IggySink {
    async fn send(
        &self,
        destination: &str,
        key: Option<&str>,
        payload: &[u8],
    ) -> Result<(), ReplayError> {
        if let Some(msg) = self.failure.message() {
            return Err(ReplayError::Sink(msg));
        }
        if !destination.is_empty() {
            let (stream, topic) = parse_stream_topic(destination)?;
            if stream != self.stream || topic != self.topic {
                return Err(ReplayError::Sink(format!(
                    "IggySink is bound to {}/{}, cannot send to {stream}/{topic}",
                    self.stream, self.topic
                )));
            }
        }

        let partition_idx = match key {
            Some(k) if !k.is_empty() => key_partition(k, self.workers.len()),
            _ => self.round_robin.fetch_add(1, Ordering::Relaxed) % self.workers.len(),
        };

        let payload_b64 = BASE64.encode(payload);
        let approx_bytes = payload_b64.len() + 32;

        self.workers[partition_idx]
            .send(WorkerCommand::Record(QueuedRecord {
                payload_b64,
                approx_bytes,
            }))
            .await
            .map_err(|_| {
                ReplayError::Sink(
                    self.failure
                        .message()
                        .unwrap_or_else(|| "Iggy partition worker stopped unexpectedly".into()),
                )
            })
    }

    async fn send_record(
        &self,
        destination: &str,
        record: &SinkRecord<'_>,
    ) -> Result<(), ReplayError> {
        self.send(destination, record.key, record.payload).await
    }

    async fn flush(&self) -> Result<(), ReplayError> {
        let mut receivers = Vec::with_capacity(self.workers.len());
        for worker in &self.workers {
            let (tx, rx) = oneshot::channel();
            if worker.send(WorkerCommand::Flush(tx)).await.is_ok() {
                receivers.push(rx);
            }
        }
        for rx in receivers {
            let _ = rx.await;
        }
        if let Some(msg) = self.failure.message() {
            return Err(ReplayError::Sink(msg));
        }
        Ok(())
    }
}

pub async fn create_iggy_sink(url: &str, destination: &str) -> Result<Box<dyn Sink>, ReplayError> {
    let username = std::env::var("IGGY_USERNAME").unwrap_or_else(|_| DEFAULT_IGGY_USERNAME.into());
    let password = std::env::var("IGGY_PASSWORD").unwrap_or_else(|_| DEFAULT_IGGY_PASSWORD.into());
    let sink = IggySink::connect(url, destination, username, password).await?;
    Ok(Box::new(sink))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn normalize_url_adds_http_and_strips_slash() {
        assert_eq!(normalize_iggy_url("127.0.0.1:3000/"), "http://127.0.0.1:3000");
        assert_eq!(
            normalize_iggy_url("http://iggy-broker:3000/"),
            "http://iggy-broker:3000"
        );
        assert_eq!(
            normalize_iggy_url("https://iggy.local:3000"),
            "https://iggy.local:3000"
        );
    }

    #[test]
    fn parse_stream_topic_handles_slash_and_default() {
        assert_eq!(
            parse_stream_topic("orders/intake").unwrap(),
            ("orders".to_string(), "intake".to_string())
        );
        assert_eq!(
            parse_stream_topic("events").unwrap(),
            ("default".to_string(), "events".to_string())
        );
        assert!(parse_stream_topic("").is_err());
        assert!(parse_stream_topic("orders/").is_err());
        assert!(parse_stream_topic("/intake").is_err());
    }

    #[tokio::test]
    async fn iggy_sink_batches_messages_across_partitions_and_flushes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let batch_requests = Arc::new(AtomicUsize::new(0));
        let batch_requests_bg = batch_requests.clone();
        let total_messages = Arc::new(AtomicUsize::new(0));
        let total_messages_bg = total_messages.clone();

        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let batch_requests_bg = batch_requests_bg.clone();
                let total_messages_bg = total_messages_bg.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 65536];
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let first_line = req.lines().next().unwrap_or("");

                    let (status, body) = if first_line.starts_with("POST /users/login") {
                        (
                            "200 OK",
                            r#"{"user_id":0,"access_token":{"token":"test-jwt"}}"#,
                        )
                    } else if first_line.starts_with("GET /streams/orders/topics/intake") {
                        ("200 OK", r#"{"id":0,"name":"intake","partitions_count":4}"#)
                    } else if first_line.starts_with("POST /streams/orders/topics/intake/messages") {
                        assert!(
                            req.contains("Authorization: Bearer test-jwt")
                                || req.contains("authorization: Bearer test-jwt")
                        );
                        assert!(req.contains(r#""kind":"partition_id""#));
                        let msg_count = req.matches(r#""payload":"#).count();
                        batch_requests_bg.fetch_add(1, Ordering::SeqCst);
                        total_messages_bg.fetch_add(msg_count, Ordering::SeqCst);
                        ("201 Created", r#"{"confirmations":[]}"#)
                    } else {
                        ("404 Not Found", r#"{"error":"not_found"}"#)
                    };

                    let resp = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                });
            }
        });

        let sink = IggySink::connect(
            &format!("http://{addr}"),
            "orders/intake",
            "iggy".into(),
            "iggy".into(),
        )
        .await
        .unwrap();

        for i in 0..40 {
            let key = format!("order-{i}");
            let payload = format!(r#"{{"order_id":"{key}"}}"#);
            sink.send("orders/intake", Some(&key), payload.as_bytes())
                .await
                .unwrap();
        }
        sink.flush().await.unwrap();

        assert_eq!(total_messages.load(Ordering::SeqCst), 40);
        // 40 messages across 4 partitions should batch into far fewer than 40 HTTP requests
        assert!(batch_requests.load(Ordering::SeqCst) <= 12);
    }
}
