//! Kafka sink tests against an in-process fake broker that speaks the real wire protocol
//! (ApiVersions, Metadata, Produce). No Docker is needed. The same checks run against a real
//! Kafka-compatible endpoint with `KAFKA_TEST_ENDPOINT=host:port KAFKA_TEST_TOPIC=name cargo
//! test -- --ignored` (the topic must exist and be empty).

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsResponse, MetadataRequest, MetadataResponse, ProduceRequest,
    ProduceResponse, RequestHeader, ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use kafka_protocol::records::RecordBatchDecoder;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use stream_replay_rs::config::ReplayConfig;
use stream_replay_rs::engine::ReplayEngine;
use stream_replay_rs::sink::kafka::{connect, partition_index, KafkaOptions};
use stream_replay_rs::sink::{Sink, SinkRecord, SinkType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

mod common;

#[derive(Debug, Clone, PartialEq)]
struct Stored {
    key: Option<Vec<u8>>,
    value: Vec<u8>,
    headers: Vec<(String, Vec<u8>)>,
    timestamp_ms: i64,
}

#[derive(Default)]
struct BrokerState {
    /// topic name and partition logs
    topic: String,
    logs: Vec<Vec<Stored>>,
    /// Produce error code returned instead of storing (0 = store).
    produce_error: i16,
    /// Never answer a Produce request.
    hang_on_produce: bool,
    produce_requests: usize,
    auto_create_seen: bool,
}

struct FakeBroker {
    addr: SocketAddr,
    state: Arc<Mutex<BrokerState>>,
}

impl FakeBroker {
    async fn start(topic: &str, partitions: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(BrokerState {
            topic: topic.to_string(),
            logs: vec![Vec::new(); partitions],
            ..Default::default()
        }));
        let shared = Arc::clone(&state);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(serve(stream, addr, Arc::clone(&shared)));
            }
        });
        Self { addr, state }
    }

    fn url(&self) -> String {
        self.addr.to_string()
    }

    fn logs(&self) -> Vec<Vec<Stored>> {
        self.state.lock().unwrap().logs.clone()
    }

    fn total(&self) -> usize {
        self.logs().iter().map(Vec::len).sum()
    }
}

async fn serve(mut stream: TcpStream, addr: SocketAddr, state: Arc<Mutex<BrokerState>>) {
    loop {
        let mut len = [0u8; 4];
        if stream.read_exact(&mut len).await.is_err() {
            return;
        }
        let mut frame = vec![0u8; i32::from_be_bytes(len) as usize];
        if stream.read_exact(&mut frame).await.is_err() {
            return;
        }
        let Some(response) = handle(&frame, addr, &state) else {
            // No answer (acks=0 or a hung broker): keep the connection open and silent.
            continue;
        };
        let mut out = (response.len() as i32).to_be_bytes().to_vec();
        out.extend_from_slice(&response);
        if stream.write_all(&out).await.is_err() {
            return;
        }
    }
}

fn handle(frame: &[u8], addr: SocketAddr, state: &Mutex<BrokerState>) -> Option<Vec<u8>> {
    let api_key = ApiKey::try_from(i16::from_be_bytes([frame[0], frame[1]])).unwrap();
    let version = i16::from_be_bytes([frame[2], frame[3]]);
    let mut buf = Bytes::copy_from_slice(frame);
    let header = RequestHeader::decode(&mut buf, api_key.request_header_version(version)).unwrap();

    let mut body = BytesMut::new();
    match api_key {
        ApiKey::ApiVersions => {
            let mut response = ApiVersionsResponse::default();
            response.api_keys = [
                (ApiKey::Produce, 3, 9),
                (ApiKey::Metadata, 0, 9),
                (ApiKey::ApiVersions, 0, 3),
            ]
            .into_iter()
            .map(|(key, min, max)| {
                let mut v = ApiVersion::default();
                v.api_key = key as i16;
                v.min_version = min;
                v.max_version = max;
                v
            })
            .collect();
            response.encode(&mut body, version).unwrap();
        }
        ApiKey::Metadata => {
            let request = MetadataRequest::decode(&mut buf, version).unwrap();
            let mut st = state.lock().unwrap();
            if request.allow_auto_topic_creation {
                st.auto_create_seen = true;
            }
            let mut broker = MetadataResponseBroker::default();
            broker.node_id = 0.into();
            broker.host = StrBytes::from_string(addr.ip().to_string());
            broker.port = addr.port() as i32;
            let known = st.topic.clone();
            let wanted: Vec<String> = match request.topics {
                None => vec![known.clone()],
                Some(topics) => topics
                    .into_iter()
                    .filter_map(|t| t.name.map(|n| n.0.to_string()))
                    .collect(),
            };
            let mut response = MetadataResponse::default();
            response.brokers = vec![broker];
            response.topics = wanted
                .into_iter()
                .map(|name| {
                    let mut topic = MetadataResponseTopic::default();
                    topic.name = Some(TopicName(StrBytes::from_string(name.clone())));
                    if name == known {
                        topic.partitions = (0..st.logs.len() as i32)
                            .map(|index| {
                                let mut p = MetadataResponsePartition::default();
                                p.partition_index = index;
                                p.leader_id = 0.into();
                                p.replica_nodes = vec![0.into()];
                                p.isr_nodes = vec![0.into()];
                                p
                            })
                            .collect();
                    } else {
                        topic.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION, never auto-created
                    }
                    topic
                })
                .collect();
            response.encode(&mut body, version).unwrap();
        }
        ApiKey::Produce => {
            let request = ProduceRequest::decode(&mut buf, version).unwrap();
            let mut st = state.lock().unwrap();
            st.produce_requests += 1;
            if st.hang_on_produce {
                return None;
            }
            let mut response = ProduceResponse::default();
            for topic in request.topic_data {
                let mut topic_response = TopicProduceResponse::default();
                topic_response.name = topic.name.clone();
                for partition in topic.partition_data {
                    let mut p = PartitionProduceResponse::default();
                    p.index = partition.index;
                    if st.produce_error != 0 {
                        p.error_code = st.produce_error;
                        p.base_offset = -1;
                    } else if let Some(mut records) = partition.records {
                        let log_len = st.logs[partition.index as usize].len() as i64;
                        p.base_offset = log_len;
                        for set in RecordBatchDecoder::decode_all(&mut records).unwrap() {
                            for record in set.records {
                                st.logs[partition.index as usize].push(Stored {
                                    key: record.key.map(|k| k.to_vec()),
                                    value: record.value.map(|v| v.to_vec()).unwrap_or_default(),
                                    headers: record
                                        .headers
                                        .into_iter()
                                        .map(|(name, value)| {
                                            (
                                                name.to_string(),
                                                value.map(|v| v.to_vec()).unwrap_or_default(),
                                            )
                                        })
                                        .collect(),
                                    timestamp_ms: record.timestamp,
                                });
                            }
                        }
                    }
                    topic_response.partition_responses.push(p);
                }
                response.responses.push(topic_response);
            }
            if request.acks == 0 {
                return None;
            }
            response.encode(&mut body, version).unwrap();
        }
        other => panic!("fake broker does not serve {other:?}"),
    }

    let mut out = BytesMut::new();
    let mut response_header = ResponseHeader::default();
    response_header.correlation_id = header.correlation_id;
    response_header
        .encode(&mut out, api_key.response_header_version(version))
        .unwrap();
    out.extend_from_slice(&body);
    Some(out.to_vec())
}

fn fast_options() -> KafkaOptions {
    KafkaOptions {
        linger: Duration::from_millis(2),
        request_timeout: Duration::from_secs(2),
        backoff_deadline: Duration::from_secs(2),
        connect_timeout: Duration::from_secs(3),
        ..KafkaOptions::default()
    }
}

#[tokio::test]
async fn n_records_sent_are_n_records_stored_with_keys_headers_and_order() {
    let broker = FakeBroker::start("orders", 3).await;
    let sink = connect(&broker.url(), "orders", fast_options())
        .await
        .unwrap();
    assert_eq!(sink.partition_count(), 3);

    let n = 600usize;
    let when = chrono::Utc::now();
    let payloads: Vec<String> = (0..n).map(|i| format!("{{\"i\":{i}}}")).collect();
    for (i, payload) in payloads.iter().enumerate() {
        let key = format!("order-{}", i % 40);
        let record = SinkRecord {
            key: Some(&key),
            payload: payload.as_bytes(),
            timestamp: when,
            headers: vec![
                ("replay.run_id".into(), "run-1".into()),
                ("replay.seq".into(), i.to_string()),
            ],
        };
        sink.send_record("orders", &record).await.unwrap();
    }
    sink.flush().await.unwrap();

    assert_eq!(sink.acked_records(), n as u64);
    assert_eq!(broker.total(), n, "N sent must be N stored");

    let logs = broker.logs();
    let mut seen = vec![false; n];
    for (partition, log) in logs.iter().enumerate() {
        let mut last_seq_per_key = std::collections::HashMap::new();
        for stored in log {
            let key = String::from_utf8(stored.key.clone().unwrap()).unwrap();
            // keys land in the partition the Java partitioner picks
            assert_eq!(partition_index(key.as_bytes(), 3), partition, "{key}");
            let seq: usize = String::from_utf8(
                stored
                    .headers
                    .iter()
                    .find(|(name, _)| name == "replay.seq")
                    .unwrap()
                    .1
                    .clone(),
            )
            .unwrap()
            .parse()
            .unwrap();
            assert!(!seen[seq], "record {seq} stored twice");
            seen[seq] = true;
            assert_eq!(stored.value, payloads[seq].as_bytes());
            assert_eq!(stored.timestamp_ms, when.timestamp_millis());
            assert!(stored
                .headers
                .iter()
                .any(|(n, v)| n == "replay.run_id" && v == b"run-1"));
            // order is kept per key
            if let Some(prev) = last_seq_per_key.insert(key, seq) {
                assert!(prev < seq, "order broken inside a partition");
            }
        }
    }
    assert!(seen.iter().all(|s| *s), "a record is missing");
    // rskafka 0.6.0 always asks the broker to auto-create missing topics and cannot turn it
    // off, so the broker must keep auto-creation disabled.
    // If a newer rskafka stops sending the flag, this check can be relaxed.
    assert!(
        broker.state.lock().unwrap().auto_create_seen,
        "expected rskafka to send allow_auto_topic_creation=true"
    );
}

#[tokio::test]
async fn records_without_a_key_are_spread_over_the_partitions() {
    let broker = FakeBroker::start("orders", 3).await;
    let sink = connect(&broker.url(), "orders", fast_options())
        .await
        .unwrap();
    for i in 0..30 {
        sink.send("orders", None, format!("{i}").as_bytes())
            .await
            .unwrap();
    }
    sink.flush().await.unwrap();
    assert_eq!(broker.total(), 30);
    for log in broker.logs() {
        assert_eq!(log.len(), 10);
    }
}

#[tokio::test]
async fn an_unknown_topic_fails_at_start_and_nothing_is_created() {
    let broker = FakeBroker::start("orders", 1).await;
    let err = match connect(&broker.url(), "missing", fast_options()).await {
        Ok(_) => panic!("an unknown topic must fail"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("does not exist"), "{err}");
    assert_eq!(broker.total(), 0);
}

#[tokio::test]
async fn a_delivery_error_is_reported_and_stops_the_run() {
    let broker = FakeBroker::start("orders", 1).await;
    broker.state.lock().unwrap().produce_error = 10; // MESSAGE_TOO_LARGE, not retried
    let sink = connect(&broker.url(), "orders", fast_options())
        .await
        .unwrap();
    sink.send("orders", Some("k"), b"x").await.unwrap();
    let err = sink.flush().await.unwrap_err().to_string();
    assert!(err.contains("produce"), "{err}");
    // after a failure the sink refuses more records instead of pretending
    assert!(sink.send("orders", Some("k"), b"y").await.is_err());
    assert_eq!(broker.total(), 0);
}

#[tokio::test]
async fn a_hung_broker_ends_with_a_timeout_not_a_hang() {
    let broker = FakeBroker::start("orders", 1).await;
    broker.state.lock().unwrap().hang_on_produce = true;
    let mut options = fast_options();
    options.request_timeout = Duration::from_millis(300);
    let sink = connect(&broker.url(), "orders", options).await.unwrap();
    sink.send("orders", Some("k"), b"x").await.unwrap();
    let started = std::time::Instant::now();
    let err = sink.flush().await.unwrap_err().to_string();
    assert!(err.contains("timed out"), "{err}");
    assert!(err.contains("may or may not"), "{err}");
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn the_sink_rejects_a_different_topic() {
    let broker = FakeBroker::start("orders", 1).await;
    let sink = connect(&broker.url(), "orders", fast_options())
        .await
        .unwrap();
    assert!(sink.send("other", None, b"x").await.is_err());
}

#[tokio::test]
async fn a_full_replay_through_the_engine_stores_every_record_with_identity_headers() {
    let broker = FakeBroker::start("orders", 2).await;

    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("events.parquet");
    common::write_parquet_events(&data, 50, 1);

    let config = ReplayConfig::builder()
        .file_path(data)
        .timestamp_column("event_ts")
        .key_column("id")
        .speed_multiplier(0.0)
        .progress(false)
        .sink(SinkType::Kafka {
            broker_url: broker.url(),
            topic: "orders".into(),
        })
        .build()
        .unwrap();
    let mut engine = ReplayEngine::new(config).await.unwrap();
    let stats = engine.run().await.unwrap();

    assert_eq!(stats.total_records, 50);
    assert_eq!(broker.total(), 50, "N records sent are N records stored");

    let mut seqs = Vec::new();
    let mut run_ids = std::collections::HashSet::new();
    for log in broker.logs() {
        for stored in log {
            let header = |name: &str| {
                String::from_utf8(
                    stored
                        .headers
                        .iter()
                        .find(|(n, _)| n == name)
                        .unwrap_or_else(|| panic!("missing header {name}"))
                        .1
                        .clone(),
                )
                .unwrap()
            };
            seqs.push(header("replay.seq").parse::<u64>().unwrap());
            run_ids.insert(header("replay.run_id"));
            assert_eq!(header("replay.loop"), "0");
            assert!(stored.key.is_some());
        }
    }
    seqs.sort_unstable();
    assert_eq!(seqs, (0..50).collect::<Vec<u64>>());
    assert_eq!(run_ids.len(), 1, "one run id per process");
}

#[tokio::test]
async fn an_engine_run_against_a_dead_endpoint_fails_instead_of_simulating() {
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
        .sink(SinkType::Kafka {
            broker_url: format!("127.0.0.1:{port}"),
            topic: "orders".into(),
        })
        .build()
        .unwrap();
    assert!(ReplayEngine::new(config).await.is_err());
}

/// Real endpoint: `KAFKA_TEST_ENDPOINT=host:port KAFKA_TEST_TOPIC=name cargo test -- --ignored`.
#[tokio::test]
#[ignore = "needs a real Kafka-compatible endpoint and an existing empty topic"]
async fn real_endpoint_accepts_a_keyed_batch() {
    let endpoint = std::env::var("KAFKA_TEST_ENDPOINT").expect("KAFKA_TEST_ENDPOINT");
    let topic = std::env::var("KAFKA_TEST_TOPIC").expect("KAFKA_TEST_TOPIC");
    let sink = connect(&endpoint, &topic, KafkaOptions::default())
        .await
        .unwrap();
    for i in 0..1000 {
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
        sink.send_record(&topic, &record).await.unwrap();
    }
    sink.flush().await.unwrap();
    assert_eq!(sink.acked_records(), 1000);
}
