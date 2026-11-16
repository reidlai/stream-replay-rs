//! Fluvio distributed streaming sink integration tests against an in-process fake Fluvio
//! SC/SPU server speaking the real Fluvio binary wire protocol (`ApiVersionsRequest`,
//! `ObjectApiWatchRequest`, and `ProduceRequest`). No Docker is needed. A live Fluvio
//! cluster can be exercised with:
//! `FLUVIO_TEST_ENDPOINT=host:9003 FLUVIO_TEST_TOPIC=orders cargo test --features fluvio -- --ignored`.

#![cfg(feature = "fluvio")]

use fluvio::dataplane::api::{Request, RequestHeader, ResponseMessage};
use fluvio::dataplane::link::versions::{
    ApiVersionKey, ApiVersionsRequest, ApiVersionsResponse, PlatformVersion,
};
use fluvio::dataplane::link::ErrorCode;
use fluvio::dataplane::record::{MemoryRecords, RecordSet, ReplicaKey};
use fluvio::dataplane::{Decoder, Encoder};
use fluvio::metadata::core::Spec;
use fluvio::metadata::objects::{
    Metadata, MetadataUpdate, ObjectApiWatchRequest, TypeBuffer, WatchResponse,
};
use fluvio::metadata::partition::{PartitionResolution, PartitionSpec, PartitionStatus};
use fluvio::metadata::spu::SpuSpec;
use fluvio::metadata::topic::{TopicResolution, TopicSpec, TopicStatus};
use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Write};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use stream_replay_rs::config::{
    sink_from_parts, ReplayConfig, DEFAULT_FLUVIO_URL, DEFAULT_SINK_URL,
};
use stream_replay_rs::engine::ReplayEngine;
use stream_replay_rs::error::ReplayError;
use stream_replay_rs::sink::fluvio::{connect, create_fluvio_sink, FluvioOptions};
use stream_replay_rs::sink::{create_sink, Sink, SinkRecord, SinkType};
use tempfile::NamedTempFile;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

mod common;

const PRODUCE_API_KEY: u16 = 0;
const PRODUCE_MAX_VERSION: i16 = 25;

#[derive(Debug, Clone, PartialEq)]
struct StoredRecord {
    topic: String,
    partition: u32,
    offset: i64,
    key: Option<String>,
    payload: Vec<u8>,
    wire_payload: Vec<u8>,
    headers: HashMap<String, String>,
}

#[derive(Default)]
struct ClusterState {
    topic: String,
    partitions: u32,
    records: Vec<StoredRecord>,
    next_offsets: HashMap<u32, i64>,
    produce_error: Option<ErrorCode>,
    hang_on_produce: bool,
    hang_on_connect: bool,
    produce_attempts: usize,
}

struct FakeFluvioCluster {
    addr: SocketAddr,
    state: Arc<Mutex<ClusterState>>,
}

impl FakeFluvioCluster {
    async fn start(topic: &str, partitions: u32) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(ClusterState {
            topic: topic.to_string(),
            partitions: partitions.max(1),
            ..Default::default()
        }));
        let shared = Arc::clone(&state);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(serve_fluvio(stream, addr, Arc::clone(&shared)));
            }
        });
        Self { addr, state }
    }

    fn endpoint(&self) -> String {
        self.addr.to_string()
    }

    fn records(&self) -> Vec<StoredRecord> {
        self.state.lock().unwrap().records.clone()
    }

    fn total(&self) -> usize {
        self.state.lock().unwrap().records.len()
    }

    fn produce_attempts(&self) -> usize {
        self.state.lock().unwrap().produce_attempts
    }
}

fn platform_version(ver: &str) -> PlatformVersion {
    let mut buf = Vec::new();
    ver.to_string().encode(&mut buf, 0).unwrap();
    let mut pv = PlatformVersion::default();
    pv.decode(&mut Cursor::new(buf), 0).unwrap();
    pv
}

fn write_framed_response<P: Encoder + Default>(header: &RequestHeader, payload: P) -> Vec<u8> {
    let version = header.api_version();
    let resp = ResponseMessage::from_header(header, payload);
    let body = resp.as_bytes(version).expect("encode response body");
    let mut frame = Vec::with_capacity(4 + body.len());
    (body.len() as i32)
        .encode(&mut frame, 0)
        .expect("encode frame len");
    frame.extend_from_slice(&body);
    frame
}

fn extract_payload_and_headers(wire: &[u8]) -> (Vec<u8>, HashMap<String, String>) {
    let mut headers = HashMap::new();
    if let Ok(serde_json::Value::Object(mut obj)) =
        serde_json::from_slice::<serde_json::Value>(wire)
    {
        let header_keys: Vec<String> = obj
            .keys()
            .filter(|k| k.starts_with("replay."))
            .cloned()
            .collect();
        for key in header_keys {
            if let Some(serde_json::Value::String(val)) = obj.remove(&key) {
                headers.insert(key, val);
            }
        }
        if let Ok(clean_payload) = serde_json::to_vec(&obj) {
            return (clean_payload, headers);
        }
    }
    (wire.to_vec(), headers)
}

async fn serve_fluvio(mut stream: TcpStream, addr: SocketAddr, state: Arc<Mutex<ClusterState>>) {
    loop {
        let Ok(frame_len) = stream.read_i32().await else {
            return;
        };
        if frame_len <= 0 {
            return;
        }
        let mut buf = vec![0u8; frame_len as usize];
        if stream.read_exact(&mut buf).await.is_err() {
            return;
        }

        if state.lock().unwrap().hang_on_connect {
            futures_hold().await;
            return;
        }

        let mut cursor = Cursor::new(&buf);
        let Ok(header) = RequestHeader::decode_from(&mut cursor, 0) else {
            return;
        };
        let api_key = header.api_key();
        let version = header.api_version();

        if api_key == ApiVersionsRequest::API_KEY {
            let resp = ApiVersionsResponse {
                error_code: ErrorCode::None,
                api_keys: vec![
                    ApiVersionKey {
                        api_key: PRODUCE_API_KEY as i16,
                        min_version: 0,
                        max_version: PRODUCE_MAX_VERSION,
                    },
                    ApiVersionKey {
                        api_key: ApiVersionsRequest::API_KEY as i16,
                        min_version: 0,
                        max_version: ApiVersionsRequest::DEFAULT_API_VERSION,
                    },
                    ApiVersionKey {
                        api_key: ObjectApiWatchRequest::API_KEY as i16,
                        min_version: ObjectApiWatchRequest::MIN_API_VERSION,
                        max_version: ObjectApiWatchRequest::DEFAULT_API_VERSION,
                    },
                ],
                platform_version: platform_version("0.50.1"),
            };
            let frame = write_framed_response(&header, resp);
            if stream.write_all(&frame).await.is_err() {
                return;
            }
            continue;
        }

        if api_key == ObjectApiWatchRequest::API_KEY {
            let mut tb = TypeBuffer::default();
            if tb.decode(&mut cursor, version).is_err() {
                return;
            }
            let (topic_name, partitions) = {
                let st = state.lock().unwrap();
                (st.topic.clone(), st.partitions)
            };

            let response_tb = if tb.is_kind_of::<SpuSpec>() {
                let spu_spec = SpuSpec::new_public_addr(0, addr.port(), "127.0.0.1".to_string());
                let mut spu_status = <SpuSpec as Spec>::Status::default();
                spu_status.set_online();
                let update = MetadataUpdate::with_all(
                    1,
                    vec![Metadata {
                        name: "spu-0".to_string(),
                        spec: spu_spec,
                        status: spu_status,
                    }],
                );
                TypeBuffer::encode::<SpuSpec, _>(WatchResponse::new(update), version).unwrap()
            } else if tb.is_kind_of::<PartitionSpec>() {
                let mut items = Vec::with_capacity(partitions as usize);
                for p in 0..partitions {
                    let replica_key = ReplicaKey::new(topic_name.clone(), p);
                    let spec = PartitionSpec::new(0, vec![0]);
                    let status = PartitionStatus::new2(
                        (0, 0, 0),
                        vec![(0, 0, 0).into()],
                        0,
                        PartitionResolution::Online,
                        0,
                    );
                    items.push(Metadata {
                        name: replica_key.to_string(),
                        spec,
                        status,
                    });
                }
                let update = MetadataUpdate::with_all(1, items);
                TypeBuffer::encode::<PartitionSpec, _>(WatchResponse::new(update), version).unwrap()
            } else if tb.is_kind_of::<TopicSpec>() {
                let spec = TopicSpec::new_computed(partitions, 1, Some(false));
                let replica_rows = (0..partitions).map(|_| vec![0]).collect();
                let status = TopicStatus::new(TopicResolution::Provisioned, replica_rows, "");
                let update = MetadataUpdate::with_all(
                    1,
                    vec![Metadata {
                        name: topic_name,
                        spec,
                        status,
                    }],
                );
                TypeBuffer::encode::<TopicSpec, _>(WatchResponse::new(update), version).unwrap()
            } else {
                continue;
            };

            let frame = write_framed_response(&header, response_tb);
            if stream.write_all(&frame).await.is_err() {
                return;
            }
            continue;
        }

        if api_key == PRODUCE_API_KEY {
            let Some(frame) = handle_produce_request(&mut cursor, &header, &state).await else {
                futures_hold().await;
                return;
            };
            if stream.write_all(&frame).await.is_err() {
                return;
            }
        }
    }
}

async fn futures_hold() {
    tokio::time::sleep(Duration::from_secs(3600)).await;
}

async fn handle_produce_request(
    cursor: &mut Cursor<&Vec<u8>>,
    header: &RequestHeader,
    state: &Mutex<ClusterState>,
) -> Option<Vec<u8>> {
    let version = header.api_version();

    // Decode ProduceRequest header fields: transactional_id (min_version = 3), isolation (i16), timeout_ms (i32)
    if version >= 3 {
        let mut _tx_id: Option<String> = None;
        let _ = _tx_id.decode(cursor, version);
    }
    let mut _isolation: i16 = 0;
    let _ = _isolation.decode(cursor, version);
    let mut _timeout_ms: i32 = 0;
    let _ = _timeout_ms.decode(cursor, version);

    let mut topic_count: u32 = 0;
    if topic_count.decode(cursor, version).is_err() {
        return None;
    }

    let mut response_bytes = Vec::new();
    header
        .correlation_id()
        .encode(&mut response_bytes, version)
        .unwrap();
    topic_count.encode(&mut response_bytes, version).unwrap();

    for _ in 0..topic_count {
        let mut topic_name = String::new();
        if topic_name.decode(cursor, version).is_err() {
            return None;
        }
        let mut part_count: u32 = 0;
        if part_count.decode(cursor, version).is_err() {
            return None;
        }

        topic_name.encode(&mut response_bytes, version).unwrap();
        part_count.encode(&mut response_bytes, version).unwrap();

        for _ in 0..part_count {
            let mut partition_index: u32 = 0;
            if partition_index.decode(cursor, version).is_err() {
                return None;
            }
            let mut record_set = RecordSet::<MemoryRecords>::default();
            if record_set.decode(cursor, version).is_err() {
                return None;
            }

            let mut st = state.lock().unwrap();
            st.produce_attempts += 1;

            if st.hang_on_produce {
                return None;
            }

            let (error_code, base_offset) = if let Some(ref err) = st.produce_error {
                (err.clone(), 0i64)
            } else if topic_name != st.topic {
                (ErrorCode::TopicNotFound, 0i64)
            } else {
                let start_offset = *st.next_offsets.get(&partition_index).unwrap_or(&0);
                let mut current_offset = start_offset;
                for batch in &record_set.batches {
                    for rec in batch.records() {
                        let key = rec
                            .key()
                            .map(|k| String::from_utf8_lossy(k.as_ref()).to_string());
                        let wire_payload = rec.value().as_ref().to_vec();
                        let (payload, headers) = extract_payload_and_headers(&wire_payload);
                        st.records.push(StoredRecord {
                            topic: topic_name.clone(),
                            partition: partition_index,
                            offset: current_offset,
                            key,
                            payload,
                            wire_payload,
                            headers,
                        });
                        current_offset += 1;
                    }
                }
                st.next_offsets.insert(partition_index, current_offset);
                (ErrorCode::None, start_offset)
            };
            drop(st);

            partition_index
                .encode(&mut response_bytes, version)
                .unwrap();
            error_code.encode(&mut response_bytes, version).unwrap();
            base_offset.encode(&mut response_bytes, version).unwrap();
            if version >= 2 {
                (-1i64).encode(&mut response_bytes, version).unwrap();
            }
            if version >= 5 {
                0i64.encode(&mut response_bytes, version).unwrap();
            }
        }
    }

    if version >= 1 {
        0i32.encode(&mut response_bytes, version).unwrap();
    }

    let mut frame = Vec::with_capacity(4 + response_bytes.len());
    (response_bytes.len() as i32).encode(&mut frame, 0).unwrap();
    frame.extend_from_slice(&response_bytes);
    Some(frame)
}

fn fast_options() -> FluvioOptions {
    FluvioOptions {
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(2),
        linger: Duration::from_millis(5),
        batch_size: 256 * 1024,
        use_spu_local_address: false,
    }
}

#[tokio::test]
async fn partitioned_replay_stores_all_records_with_keys_and_replay_headers() {
    let cluster = FakeFluvioCluster::start("orders", 3).await;
    let sink = connect(&cluster.endpoint(), "orders", fast_options())
        .await
        .unwrap();
    assert_eq!(sink.topic(), "orders");
    assert_eq!(sink.endpoint(), cluster.endpoint());

    let n = 60usize;
    let when = chrono::Utc::now();
    let expected_ts = when.to_rfc3339();
    for i in 0..n {
        let key = format!("order-{}", i % 12);
        let payload = format!("{{\"seq\":{i}}}");
        let record = SinkRecord {
            key: Some(&key),
            payload: payload.as_bytes(),
            timestamp: when,
            headers: vec![
                ("replay.run_id".into(), "run-fluvio-42".into()),
                ("replay.seq".into(), i.to_string()),
                ("replay.loop".into(), "0".into()),
            ],
        };
        sink.send_record("orders", &record).await.unwrap();
    }
    sink.flush().await.unwrap();

    assert_eq!(sink.acked_records(), n as u64);
    assert_eq!(cluster.total(), n, "N sent must be N stored");

    let mut partitions_seen = HashSet::new();
    let mut key_to_partition: HashMap<String, u32> = HashMap::new();

    for (i, stored) in cluster.records().iter().enumerate() {
        let expected_key = format!("order-{}", i % 12);
        assert_eq!(stored.topic, "orders");
        assert_eq!(stored.key.as_deref(), Some(expected_key.as_str()));
        assert_eq!(stored.payload, format!("{{\"seq\":{i}}}").as_bytes());
        assert_eq!(
            stored.headers.get("replay.run_id").map(String::as_str),
            Some("run-fluvio-42")
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
            Some(expected_key.as_str())
        );
        assert_eq!(
            stored.headers.get("replay.timestamp").map(String::as_str),
            Some(expected_ts.as_str())
        );

        partitions_seen.insert(stored.partition);
        if let Some(&prev_part) = key_to_partition.get(&expected_key) {
            assert_eq!(
                stored.partition, prev_part,
                "same RecordKey must route deterministically to the same partition"
            );
        } else {
            key_to_partition.insert(expected_key, stored.partition);
        }
    }
    assert!(
        partitions_seen.len() > 1,
        "12 distinct RecordKeys across 3 partitions must use more than 1 partition"
    );
}

#[tokio::test]
async fn unreachable_endpoint_fails_within_connect_timeout() {
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let options = FluvioOptions {
        connect_timeout: Duration::from_millis(500),
        request_timeout: Duration::from_secs(1),
        ..Default::default()
    };
    let started = std::time::Instant::now();
    let err = match connect(&format!("127.0.0.1:{port}"), "orders", options).await {
        Ok(_) => panic!("unreachable Fluvio endpoint must fail"),
        Err(e) => e,
    };
    assert!(matches!(err, ReplayError::Sink(_)), "{err:?}");
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn hung_endpoint_on_connect_times_out_within_connect_timeout() {
    let cluster = FakeFluvioCluster::start("orders", 1).await;
    cluster.state.lock().unwrap().hang_on_connect = true;

    let options = FluvioOptions {
        connect_timeout: Duration::from_millis(300),
        request_timeout: Duration::from_secs(1),
        ..Default::default()
    };
    let started = std::time::Instant::now();
    let err = match connect(&cluster.endpoint(), "orders", options).await {
        Ok(_) => panic!("hung Fluvio endpoint must time out"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("did not answer in time") || msg.contains("timed out"),
        "expected timeout diagnostic, got: {msg}"
    );
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn missing_topic_fails_connect_immediately() {
    let cluster = FakeFluvioCluster::start("orders", 1).await;
    let err = match connect(&cluster.endpoint(), "missing-topic", fast_options()).await {
        Ok(_) => panic!("missing Fluvio topic must fail"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("topic") || msg.to_lowercase().contains("not found"),
        "expected missing topic diagnostic, got: {msg}"
    );
}

#[tokio::test]
async fn spu_produce_rejection_latches_failure_and_stops_run() {
    let cluster = FakeFluvioCluster::start("orders", 1).await;
    cluster.state.lock().unwrap().produce_error = Some(ErrorCode::StorageError);

    let sink = connect(&cluster.endpoint(), "orders", fast_options())
        .await
        .unwrap();
    let err = sink
        .send("orders", Some("k1"), b"{\"a\":1}")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.to_lowercase().contains("storage")
            || err.to_lowercase().contains("spu")
            || err.to_lowercase().contains("failed"),
        "expected SPU error diagnostic, got: {err}"
    );
    assert_eq!(sink.acked_records(), 0);
    assert_eq!(cluster.total(), 0);

    // Subsequent sends and flush must fail immediately because the sink is latched
    assert!(sink.send("orders", Some("k2"), b"{\"a\":2}").await.is_err());
    assert!(sink.flush().await.is_err());
}

#[tokio::test]
async fn hung_spu_on_produce_times_out_and_latches_failure() {
    let cluster = FakeFluvioCluster::start("orders", 1).await;
    cluster.state.lock().unwrap().hang_on_produce = true;

    let options = FluvioOptions {
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_millis(350),
        linger: Duration::from_millis(5),
        batch_size: 256 * 1024,
        use_spu_local_address: false,
    };
    let sink = connect(&cluster.endpoint(), "orders", options)
        .await
        .unwrap();

    let started = std::time::Instant::now();
    let err = sink
        .send("orders", Some("k1"), b"{\"x\":1}")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("timed out"),
        "expected timeout diagnostic, got: {err}"
    );
    assert!(err.contains("may or may not"), "{err}");
    assert!(started.elapsed() < Duration::from_secs(5));

    // Subsequent sends and flush fail immediately due to latched failure
    assert!(sink.send("orders", Some("k2"), b"{\"x\":2}").await.is_err());
    assert!(sink.flush().await.is_err());
}

#[tokio::test]
async fn mismatched_topic_and_invalid_headers_are_rejected_and_latched() {
    let cluster = FakeFluvioCluster::start("orders", 1).await;
    let sink = connect(&cluster.endpoint(), "orders", fast_options())
        .await
        .unwrap();

    // Empty topic falls back to configured topic
    sink.send("", None, b"{\"ok\":true}").await.unwrap();
    assert_eq!(cluster.total(), 1);
    assert_eq!(cluster.records()[0].key, None);

    let attempts_before = cluster.produce_attempts();
    let err = sink.send("other-topic", Some("k"), b"x").await.unwrap_err();
    assert!(matches!(err, ReplayError::Sink(_)));
    assert_eq!(
        cluster.produce_attempts(),
        attempts_before,
        "mismatched topic must be rejected before network produce"
    );

    // Latched after mismatch
    assert!(sink.send("orders", None, b"after-mismatch").await.is_err());
    assert!(sink.flush().await.is_err());

    // Invalid header on a fresh sink is rejected before network I/O and latches failure
    let sink2 = connect(&cluster.endpoint(), "orders", fast_options())
        .await
        .unwrap();
    let bad_record = SinkRecord {
        key: Some("k1"),
        payload: b"{}",
        timestamp: chrono::Utc::now(),
        headers: vec![("bad\r\nheader".into(), "v".into())],
    };
    assert!(sink2.send_record("orders", &bad_record).await.is_err());
    assert!(sink2.send("orders", None, b"{}").await.is_err());
    assert!(sink2.flush().await.is_err());
}

#[tokio::test]
async fn full_replay_engine_parquet_run_publishes_all_records_with_identity_headers() {
    let cluster = FakeFluvioCluster::start("orders", 2).await;

    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("events.parquet");
    common::write_parquet_events(&data, 40, 1);

    let config = ReplayConfig::builder()
        .file_path(data)
        .timestamp_column("event_ts")
        .key_column("id")
        .speed_multiplier(0.0)
        .progress(false)
        .fluvio_sink(cluster.endpoint(), "orders")
        .build()
        .unwrap();
    let mut engine = ReplayEngine::new(config).await.unwrap();
    let stats = engine.run().await.unwrap();

    assert_eq!(stats.total_records, 40);
    assert_eq!(cluster.total(), 40, "N records sent are N records stored");

    let mut seqs = Vec::new();
    let mut run_ids = HashSet::new();
    for stored in cluster.records() {
        let header = |name: &str| {
            stored
                .headers
                .get(name)
                .unwrap_or_else(|| panic!("missing header {name}"))
                .clone()
        };
        let seq = header("replay.seq").parse::<u64>().unwrap();
        seqs.push(seq);
        run_ids.insert(header("replay.run_id"));
        assert_eq!(header("replay.loop"), "0");
        assert!(stored.headers.contains_key("replay.key"));
        assert!(stored.headers.contains_key("replay.timestamp"));
        assert_eq!(
            stored.key.as_deref(),
            stored.headers.get("replay.key").map(String::as_str)
        );

        let parsed: serde_json::Value = serde_json::from_slice(&stored.payload).unwrap();
        assert!(parsed.get("id").is_some());
        assert!(parsed.get("event_ts").is_some());
        assert!(parsed.get("value").is_some());
    }
    seqs.sort_unstable();
    assert_eq!(seqs, (0..40).collect::<Vec<u64>>());
    assert_eq!(run_ids.len(), 1, "one run id per process");
}

#[tokio::test]
async fn engine_run_against_dead_fluvio_endpoint_fails_instead_of_simulating() {
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
        .sink(SinkType::Fluvio {
            endpoint: format!("127.0.0.1:{port}"),
            topic: "orders".into(),
        })
        .build()
        .unwrap();
    assert!(ReplayEngine::new(config).await.is_err());

    let sink_res = create_sink(&SinkType::Fluvio {
        endpoint: format!("127.0.0.1:{port}"),
        topic: "orders".into(),
    })
    .await;
    assert!(sink_res.is_err());
    assert!(create_fluvio_sink(&format!("127.0.0.1:{port}"), "orders")
        .await
        .is_err());
}

#[tokio::test]
async fn config_profile_and_connect_validate_endpoints_and_topics() {
    // Default URL normalization
    let sink = sink_from_parts("fluvio", DEFAULT_SINK_URL.into(), "orders".into()).unwrap();
    assert_eq!(
        sink,
        SinkType::Fluvio {
            endpoint: DEFAULT_FLUVIO_URL.into(),
            topic: "orders".into(),
        }
    );
    let sink_empty_url = sink_from_parts("fluvio", "".into(), "  orders  ".into()).unwrap();
    assert_eq!(
        sink_empty_url,
        SinkType::Fluvio {
            endpoint: DEFAULT_FLUVIO_URL.into(),
            topic: "orders".into(),
        }
    );

    // Reject whitespace-only endpoint or empty/whitespace topic in sink_from_parts
    assert!(matches!(
        sink_from_parts("fluvio", "   ".into(), "orders".into()).unwrap_err(),
        ReplayError::Config(_)
    ));
    for invalid_topic in ["", "   "] {
        assert!(matches!(
            sink_from_parts("fluvio", DEFAULT_FLUVIO_URL.into(), invalid_topic.into()).unwrap_err(),
            ReplayError::Config(_)
        ));
    }

    // Builder trims and validates via fluvio_sink and sink()
    let trimmed_cfg = ReplayConfig::builder()
        .file_path("events.csv".into())
        .fluvio_sink("  127.0.0.1:9003  ", "  orders  ")
        .build()
        .unwrap();
    assert_eq!(
        trimmed_cfg.sink,
        SinkType::Fluvio {
            endpoint: "127.0.0.1:9003".into(),
            topic: "orders".into(),
        }
    );

    let trimmed_via_sink = ReplayConfig::builder()
        .file_path("events.csv".into())
        .sink(SinkType::Fluvio {
            endpoint: "  127.0.0.1:9003  ".into(),
            topic: "  orders  ".into(),
        })
        .build()
        .unwrap();
    assert_eq!(
        trimmed_via_sink.sink,
        SinkType::Fluvio {
            endpoint: "127.0.0.1:9003".into(),
            topic: "orders".into(),
        }
    );

    for invalid in ["", "   "] {
        assert!(ReplayConfig::builder()
            .file_path("events.csv".into())
            .fluvio_sink(invalid, "orders")
            .build()
            .is_err());
        assert!(ReplayConfig::builder()
            .file_path("events.csv".into())
            .fluvio_sink("127.0.0.1:9003", invalid)
            .build()
            .is_err());
        assert!(connect(invalid, "orders", fast_options()).await.is_err());
        assert!(connect("127.0.0.1:9003", invalid, fast_options())
            .await
            .is_err());
    }
    assert!(connect(
        "127.0.0.1:9003",
        "orders",
        FluvioOptions {
            batch_size: 0,
            ..fast_options()
        }
    )
    .await
    .is_err());

    // Sectioned TOML profile normalizes default URL and loads destination
    let mut sectioned = NamedTempFile::new().unwrap();
    write!(
        sectioned,
        r#"
[dataset]
input_path = "events.csv"

[sink]
default_type = "fluvio"
destination = "orders"
"#
    )
    .unwrap();
    let cfg = ReplayConfig::from_profile_file(sectioned.path()).unwrap();
    assert_eq!(
        cfg.sink,
        SinkType::Fluvio {
            endpoint: DEFAULT_FLUVIO_URL.into(),
            topic: "orders".into(),
        }
    );

    // Sectioned TOML profile with empty destination or empty default_url is rejected
    let mut bad_profile = NamedTempFile::new().unwrap();
    write!(
        bad_profile,
        r#"
[dataset]
input_path = "events.csv"

[sink]
default_type = "fluvio"
destination = "   "
"#
    )
    .unwrap();
    assert!(ReplayConfig::from_profile_file(bad_profile.path()).is_err());

    let mut bad_url_profile = NamedTempFile::new().unwrap();
    write!(
        bad_url_profile,
        r#"
[dataset]
input_path = "events.csv"

[sink]
default_type = "fluvio"
default_url = "   "
destination = "orders"
"#
    )
    .unwrap();
    assert!(ReplayConfig::from_profile_file(bad_url_profile.path()).is_err());
}

/// Real cluster: `FLUVIO_TEST_ENDPOINT=host:9003 FLUVIO_TEST_TOPIC=orders cargo test --features fluvio -- --ignored`.
#[tokio::test]
#[ignore = "needs a live Fluvio cluster and an existing topic matching FLUVIO_TEST_TOPIC"]
async fn real_endpoint_accepts_a_keyed_batch() {
    let endpoint = std::env::var("FLUVIO_TEST_ENDPOINT").expect("FLUVIO_TEST_ENDPOINT");
    let topic = std::env::var("FLUVIO_TEST_TOPIC").expect("FLUVIO_TEST_TOPIC");
    let sink = connect(&endpoint, &topic, FluvioOptions::default())
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
                ("replay.run_id".into(), "real-fluvio-test".into()),
                ("replay.seq".into(), i.to_string()),
                ("replay.loop".into(), "0".into()),
            ],
        };
        sink.send_record(&topic, &record).await.unwrap();
    }
    sink.flush().await.unwrap();
    assert_eq!(sink.acked_records(), 200);
}
