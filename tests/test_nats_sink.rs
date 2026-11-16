//! NATS JetStream sink tests against an in-process fake server speaking the real NATS
//! wire protocol (`INFO`, `CONNECT`, `PING`/`PONG`, `SUB`, `HPUB`/`PUB`, `MSG`/`HMSG`).
//! No Docker is needed. A real NATS JetStream endpoint can be exercised with
//! `NATS_TEST_ENDPOINT=nats://host:4222 NATS_TEST_SUBJECT=orders.events cargo test --features nats -- --ignored`.

#![cfg(feature = "nats")]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use stream_replay_rs::config::{sink_from_parts, ReplayConfig, DEFAULT_NATS_URL, DEFAULT_SINK_URL};
use stream_replay_rs::engine::ReplayEngine;
use stream_replay_rs::error::ReplayError;
use stream_replay_rs::sink::nats::{connect, NatsOptions};
use stream_replay_rs::sink::{create_sink, Sink, SinkRecord, SinkType};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

mod common;

#[derive(Debug, Clone, PartialEq)]
struct StoredMessage {
    subject: String,
    payload: Vec<u8>,
    headers: HashMap<String, String>,
}

#[derive(Default)]
struct BrokerState {
    stream_subject: String,
    messages: Vec<StoredMessage>,
    publish_error: Option<(u16, u16, String)>,
    hang_on_publish: bool,
    publish_attempts: usize,
}

struct FakeNatsBroker {
    addr: SocketAddr,
    state: Arc<Mutex<BrokerState>>,
}

impl FakeNatsBroker {
    async fn start(stream_subject: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(BrokerState {
            stream_subject: stream_subject.to_string(),
            ..Default::default()
        }));
        let shared = Arc::clone(&state);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(serve_nats(stream, addr, Arc::clone(&shared)));
            }
        });
        Self { addr, state }
    }

    fn url(&self) -> String {
        format!("nats://{}", self.addr)
    }

    fn messages(&self) -> Vec<StoredMessage> {
        self.state.lock().unwrap().messages.clone()
    }

    fn total(&self) -> usize {
        self.state.lock().unwrap().messages.len()
    }

    fn publish_attempts(&self) -> usize {
        self.state.lock().unwrap().publish_attempts
    }
}

fn subject_matches(pattern: &str, subject: &str) -> bool {
    if pattern == subject || pattern == ">" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix(".>") {
        return subject == prefix
            || subject
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.starts_with('.'));
    }
    if let Some(prefix) = pattern.strip_suffix(".*") {
        if let Some(rest) = subject.strip_prefix(prefix) {
            if let Some(tail) = rest.strip_prefix('.') {
                return !tail.is_empty() && !tail.contains('.');
            }
        }
    }
    false
}

fn find_sid(subs: &[(String, String)], reply_to: &str) -> String {
    subs.iter()
        .find(|(pat, _)| subject_matches(pat, reply_to))
        .map(|(_, sid)| sid.clone())
        .or_else(|| subs.first().map(|(_, sid)| sid.clone()))
        .unwrap_or_else(|| "1".to_string())
}

fn parse_header_block(raw: &[u8]) -> HashMap<String, String> {
    let text = String::from_utf8_lossy(raw);
    let mut map = HashMap::new();
    for (idx, line) in text.split("\r\n").enumerate() {
        if idx == 0 || line.is_empty() {
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            map.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    map
}

async fn serve_nats(stream: TcpStream, addr: SocketAddr, state: Arc<Mutex<BrokerState>>) {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    let info = format!(
        "INFO {{\"server_id\":\"fake-nats\",\"server_name\":\"fake-nats\",\"version\":\"2.10.0\",\"proto\":1,\"host\":\"127.0.0.1\",\"port\":{},\"headers\":true,\"max_payload\":1048576,\"jetstream\":true}}\r\n",
        addr.port()
    );
    if write_half.write_all(info.as_bytes()).await.is_err() {
        return;
    }

    let mut subs: Vec<(String, String)> = Vec::new();
    let mut line = String::new();

    loop {
        line.clear();
        let Ok(n) = reader.read_line(&mut line).await else {
            return;
        };
        if n == 0 {
            return;
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            continue;
        }

        if trimmed == "PING" {
            if write_half.write_all(b"PONG\r\n").await.is_err() {
                return;
            }
            continue;
        }
        if trimmed.starts_with("CONNECT ") || trimmed == "PONG" || trimmed.starts_with("UNSUB ") {
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("SUB ") {
            let parts: Vec<&str> = rest.split_whitespace().collect();
            if parts.len() == 2 {
                subs.push((parts[0].to_string(), parts[1].to_string()));
            } else if parts.len() >= 3 {
                subs.push((parts[0].to_string(), parts[2].to_string()));
            }
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("HPUB ") {
            let parts: Vec<&str> = rest.split_whitespace().collect();
            let (subject, reply_to, hdr_len, total_len) = match parts.as_slice() {
                [subj, hlen, tlen] => (
                    (*subj).to_string(),
                    None,
                    hlen.parse::<usize>().unwrap(),
                    tlen.parse::<usize>().unwrap(),
                ),
                [subj, reply, hlen, tlen] => (
                    (*subj).to_string(),
                    Some((*reply).to_string()),
                    hlen.parse::<usize>().unwrap(),
                    tlen.parse::<usize>().unwrap(),
                ),
                other => panic!("unexpected HPUB args: {other:?}"),
            };
            let mut buf = vec![0u8; total_len];
            if reader.read_exact(&mut buf).await.is_err() {
                return;
            }
            let mut crlf = [0u8; 2];
            if reader.read_exact(&mut crlf).await.is_err() {
                return;
            }
            let headers = parse_header_block(&buf[..hdr_len]);
            let payload = buf[hdr_len..total_len].to_vec();

            if let Some(reply) = reply_to {
                let sid = find_sid(&subs, &reply);
                let out = handle_publish(&state, subject, payload, headers, &reply, &sid);
                if let Some(frame) = out {
                    if write_half.write_all(&frame).await.is_err() {
                        return;
                    }
                }
            }
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("PUB ") {
            let parts: Vec<&str> = rest.split_whitespace().collect();
            let (subject, reply_to, total_len) = match parts.as_slice() {
                [subj, tlen] => ((*subj).to_string(), None, tlen.parse::<usize>().unwrap()),
                [subj, reply, tlen] => (
                    (*subj).to_string(),
                    Some((*reply).to_string()),
                    tlen.parse::<usize>().unwrap(),
                ),
                other => panic!("unexpected PUB args: {other:?}"),
            };
            let mut payload = vec![0u8; total_len];
            if reader.read_exact(&mut payload).await.is_err() {
                return;
            }
            let mut crlf = [0u8; 2];
            if reader.read_exact(&mut crlf).await.is_err() {
                return;
            }
            if let Some(reply) = reply_to {
                let sid = find_sid(&subs, &reply);
                let out = handle_publish(&state, subject, payload, HashMap::new(), &reply, &sid);
                if let Some(frame) = out {
                    if write_half.write_all(&frame).await.is_err() {
                        return;
                    }
                }
            }
            continue;
        }
    }
}

fn handle_publish(
    state: &Mutex<BrokerState>,
    subject: String,
    payload: Vec<u8>,
    headers: HashMap<String, String>,
    reply_to: &str,
    sid: &str,
) -> Option<Vec<u8>> {
    let mut st = state.lock().unwrap();
    st.publish_attempts += 1;

    if st.hang_on_publish {
        return None;
    }

    if subject != st.stream_subject {
        let hdr = "NATS/1.0 503 No Responders\r\n\r\n";
        let frame = format!(
            "HMSG {reply_to} {sid} {len} {len}\r\n{hdr}\r\n",
            len = hdr.len()
        );
        return Some(frame.into_bytes());
    }

    if let Some((code, err_code, ref desc)) = st.publish_error {
        let body = serde_json::json!({
            "error": {
                "code": code,
                "err_code": err_code,
                "description": desc,
            }
        })
        .to_string();
        let frame = format!("MSG {reply_to} {sid} {}\r\n{body}\r\n", body.len());
        return Some(frame.into_bytes());
    }

    st.messages.push(StoredMessage {
        subject,
        payload,
        headers,
    });
    let seq = st.messages.len();
    let body = format!("{{\"stream\":\"orders\",\"seq\":{seq}}}");
    let frame = format!("MSG {reply_to} {sid} {}\r\n{body}\r\n", body.len());
    Some(frame.into_bytes())
}

fn fast_options() -> NatsOptions {
    NatsOptions {
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(2),
    }
}

#[tokio::test]
async fn n_records_sent_are_n_records_stored_with_keys_headers_and_order() {
    let broker = FakeNatsBroker::start("orders.events").await;
    let sink = connect(&broker.url(), "orders.events", fast_options())
        .await
        .unwrap();
    assert_eq!(sink.subject(), "orders.events");

    let n = 120usize;
    let when = chrono::Utc::now();
    let expected_ts = when.to_rfc3339();
    for i in 0..n {
        let key = format!("order-{}", i % 15);
        let payload = format!("{{\"seq\":{i}}}");
        let record = SinkRecord {
            key: Some(&key),
            payload: payload.as_bytes(),
            timestamp: when,
            headers: vec![
                ("replay.run_id".into(), "run-42".into()),
                ("replay.seq".into(), i.to_string()),
                ("replay.loop".into(), "0".into()),
            ],
        };
        sink.send_record("orders.events", &record).await.unwrap();
    }
    sink.flush().await.unwrap();

    assert_eq!(sink.acked_records(), n as u64);
    assert_eq!(broker.total(), n, "N sent must be N stored");

    for (i, stored) in broker.messages().iter().enumerate() {
        assert_eq!(stored.subject, "orders.events");
        assert_eq!(stored.payload, format!("{{\"seq\":{i}}}").as_bytes());
        assert_eq!(
            stored.headers.get("replay.run_id").map(String::as_str),
            Some("run-42")
        );
        assert_eq!(
            stored.headers.get("replay.seq").map(String::as_str),
            Some(i.to_string().as_str())
        );
        assert_eq!(
            stored.headers.get("replay.loop").map(String::as_str),
            Some("0")
        );
        assert_eq!(
            stored.headers.get("replay.key").map(String::as_str),
            Some(format!("order-{}", i % 15).as_str())
        );
        assert_eq!(
            stored.headers.get("replay.timestamp").map(String::as_str),
            Some(expected_ts.as_str())
        );
        assert_eq!(
            stored.headers.get("Nats-Msg-Id").map(String::as_str),
            Some(format!("run-42:{i}").as_str())
        );
    }
}

#[tokio::test]
async fn unreachable_endpoint_fails_within_connect_timeout() {
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let options = NatsOptions {
        connect_timeout: Duration::from_millis(500),
        request_timeout: Duration::from_secs(1),
    };
    let started = std::time::Instant::now();
    let err = match connect(&format!("127.0.0.1:{port}"), "orders.events", options).await {
        Ok(_) => panic!("unreachable NATS endpoint must fail"),
        Err(e) => e,
    };
    assert!(matches!(err, ReplayError::Sink(_)), "{err:?}");
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn missing_stream_no_responders_fails_immediately() {
    let broker = FakeNatsBroker::start("orders.events").await;
    let sink = connect(&broker.url(), "missing.subject", fast_options())
        .await
        .unwrap();

    let err = sink
        .send("missing.subject", Some("k1"), b"{\"a\":1}")
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("no responders")
            || msg.to_lowercase().contains("no stream")
            || msg.to_lowercase().contains("missing"),
        "expected missing stream / no responders diagnostic, got: {msg}"
    );
    assert_eq!(sink.acked_records(), 0);
    assert_eq!(broker.total(), 0);

    // Subsequent calls must fail because the sink latched into a failed state
    assert!(sink
        .send("missing.subject", None, b"{\"a\":2}")
        .await
        .is_err());
    assert!(sink.flush().await.is_err());
}

#[tokio::test]
async fn broker_publish_error_is_recorded_and_stops_the_run() {
    let broker = FakeNatsBroker::start("orders.events").await;
    broker.state.lock().unwrap().publish_error =
        Some((500, 10077, "maximum bytes exceeded".to_string()));

    let sink = connect(&broker.url(), "orders.events", fast_options())
        .await
        .unwrap();
    let err = sink
        .send("orders.events", Some("k"), b"payload")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("PubAck") || err.contains("maximum bytes"),
        "{err}"
    );
    assert_eq!(sink.acked_records(), 0);
    assert_eq!(broker.total(), 0);

    // Refuses subsequent sends and flush after the first delivery failure
    assert!(sink
        .send("orders.events", Some("k"), b"next")
        .await
        .is_err());
    assert!(sink.flush().await.is_err());
}

#[tokio::test]
async fn a_hung_broker_on_publish_ends_with_a_timeout_not_a_hang() {
    let broker = FakeNatsBroker::start("orders.events").await;
    broker.state.lock().unwrap().hang_on_publish = true;

    let options = NatsOptions {
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_millis(300),
    };
    let sink = connect(&broker.url(), "orders.events", options)
        .await
        .unwrap();

    let started = std::time::Instant::now();
    let err = sink
        .send("orders.events", Some("k"), b"x")
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("timed out"), "{err}");
    assert!(err.contains("may or may not"), "{err}");
    assert!(started.elapsed() < Duration::from_secs(5));

    // Subsequent sends and flush must fail immediately
    assert!(sink.send("orders.events", Some("k"), b"y").await.is_err());
    assert!(sink.flush().await.is_err());
}

#[tokio::test]
async fn the_sink_rejects_a_mismatched_subject_before_network_io() {
    let broker = FakeNatsBroker::start("orders.events").await;
    let sink = connect(&broker.url(), "orders.events", fast_options())
        .await
        .unwrap();

    // Empty topic falls back to the sink's configured subject
    sink.send("", Some("k0"), b"{\"ok\":true}").await.unwrap();
    assert_eq!(broker.total(), 1);

    let err = sink.send("other.subject", None, b"x").await.unwrap_err();
    assert!(matches!(err, ReplayError::Sink(_)));
    assert_eq!(
        broker.publish_attempts(),
        1,
        "mismatched subject must be rejected before network I/O"
    );

    // Failure is latched after subject mismatch
    assert!(sink
        .send("orders.events", None, b"after-mismatch")
        .await
        .is_err());
    assert!(sink.flush().await.is_err());
}

#[tokio::test]
async fn invalid_header_is_rejected_and_latches_failure() {
    let broker = FakeNatsBroker::start("orders.events").await;
    let sink = connect(&broker.url(), "orders.events", fast_options())
        .await
        .unwrap();

    let bad_record = SinkRecord {
        key: Some("k1"),
        payload: b"{}",
        timestamp: chrono::Utc::now(),
        headers: vec![("bad header\r\nname".into(), "val".into())],
    };
    let err = sink
        .send_record("orders.events", &bad_record)
        .await
        .unwrap_err();
    assert!(matches!(err, ReplayError::Sink(_)));
    assert_eq!(broker.publish_attempts(), 0);

    // Subsequent sends and flush fail because the invalid header latched failure
    assert!(sink.send("orders.events", None, b"{}").await.is_err());
    assert!(sink.flush().await.is_err());
}

#[tokio::test]
async fn a_full_replay_through_the_engine_publishes_every_record_with_identity_headers() {
    let broker = FakeNatsBroker::start("orders.events").await;

    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("events.parquet");
    common::write_parquet_events(&data, 50, 1);

    let config = ReplayConfig::builder()
        .file_path(data)
        .timestamp_column("event_ts")
        .key_column("id")
        .speed_multiplier(0.0)
        .progress(false)
        .nats_sink(broker.url(), "orders.events")
        .build()
        .unwrap();
    let mut engine = ReplayEngine::new(config).await.unwrap();
    let stats = engine.run().await.unwrap();

    assert_eq!(stats.total_records, 50);
    assert_eq!(broker.total(), 50, "N records sent are N records stored");

    let mut seqs = Vec::new();
    let mut run_ids = std::collections::HashSet::new();
    for stored in broker.messages() {
        let header = |name: &str| {
            stored
                .headers
                .get(name)
                .unwrap_or_else(|| panic!("missing header {name}"))
                .clone()
        };
        seqs.push(header("replay.seq").parse::<u64>().unwrap());
        run_ids.insert(header("replay.run_id"));
        assert_eq!(header("replay.loop"), "0");
        assert!(stored.headers.contains_key("replay.key"));
        assert!(stored.headers.contains_key("replay.timestamp"));
        assert!(stored.headers.contains_key("Nats-Msg-Id"));
    }
    seqs.sort_unstable();
    assert_eq!(seqs, (0..50).collect::<Vec<u64>>());
    assert_eq!(run_ids.len(), 1, "one run id per process");
}

#[tokio::test]
async fn an_engine_run_against_a_dead_nats_endpoint_fails_instead_of_simulating() {
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("events.parquet");
    common::write_parquet_events(&data, 5, 1);
    let config = ReplayConfig::builder()
        .file_path(data)
        .timestamp_column("event_ts")
        .progress(false)
        .sink(SinkType::Nats {
            url: format!("127.0.0.1:{port}"),
            subject: "orders.events".into(),
        })
        .build()
        .unwrap();
    assert!(ReplayEngine::new(config).await.is_err());

    let sink_res = create_sink(&SinkType::Nats {
        url: format!("127.0.0.1:{port}"),
        subject: "orders.events".into(),
    })
    .await;
    assert!(sink_res.is_err());
}

#[tokio::test]
async fn config_and_connect_validate_urls_and_subjects() {
    let sink = sink_from_parts("nats", DEFAULT_SINK_URL.into(), "orders.events".into()).unwrap();
    assert_eq!(
        sink,
        SinkType::Nats {
            url: DEFAULT_NATS_URL.into(),
            subject: "orders.events".into(),
        }
    );

    let err = sink_from_parts("nats", DEFAULT_NATS_URL.into(), "   ".into()).unwrap_err();
    assert!(matches!(err, ReplayError::Config(_)));

    // ReplayConfigBuilder::nats_sink trims URL and subject, and rejects blank values
    let trimmed_cfg = ReplayConfig::builder()
        .file_path("events.csv".into())
        .nats_sink("  nats://127.0.0.1:4222  ", "  orders.events  ")
        .build()
        .unwrap();
    assert_eq!(
        trimmed_cfg.sink,
        SinkType::Nats {
            url: "nats://127.0.0.1:4222".into(),
            subject: "orders.events".into(),
        }
    );

    assert!(ReplayConfig::builder()
        .file_path("events.csv".into())
        .nats_sink("   ", "orders.events")
        .build()
        .is_err());
    assert!(ReplayConfig::builder()
        .file_path("events.csv".into())
        .nats_sink("nats://127.0.0.1:4222", "   ")
        .build()
        .is_err());

    // connect rejects blank URL or blank subject directly
    assert!(connect("   ", "orders.events", fast_options())
        .await
        .is_err());
    assert!(connect("nats://127.0.0.1:4222", "   ", fast_options())
        .await
        .is_err());

    // subject_matches enforces token boundary for `.>`
    assert!(subject_matches("_INBOX.abc.>", "_INBOX.abc.123"));
    assert!(!subject_matches("_INBOX.abc.>", "_INBOX.abcdef"));
}

/// Real endpoint: `NATS_TEST_ENDPOINT=nats://host:4222 NATS_TEST_SUBJECT=orders.events cargo test --features nats -- --ignored`.
#[tokio::test]
#[ignore = "needs a real NATS JetStream endpoint and an existing stream matching NATS_TEST_SUBJECT"]
async fn real_endpoint_accepts_a_keyed_batch() {
    let endpoint = std::env::var("NATS_TEST_ENDPOINT").expect("NATS_TEST_ENDPOINT");
    let subject = std::env::var("NATS_TEST_SUBJECT").expect("NATS_TEST_SUBJECT");
    let sink = connect(&endpoint, &subject, NatsOptions::default())
        .await
        .unwrap();
    for i in 0..200 {
        let key = format!("k{}", i % 20);
        let payload = format!("{{\"i\":{i}}}");
        let record = SinkRecord {
            key: Some(&key),
            payload: payload.as_bytes(),
            timestamp: chrono::Utc::now(),
            headers: vec![
                ("replay.run_id".into(), "real-endpoint-test".into()),
                ("replay.seq".into(), i.to_string()),
                ("replay.loop".into(), "0".into()),
            ],
        };
        sink.send_record(&subject, &record).await.unwrap();
    }
    sink.flush().await.unwrap();
    assert_eq!(sink.acked_records(), 200);
}
