//! Eclipse iceoryx2 zero-copy shared-memory IPC sink integration tests.
//!
//! Opens real `iceoryx2` IPC subscribers in-process to verify zero-copy `[u8]` slice
//! loan and delivery, dynamic slice reallocation, static allocation overflow failure
//! latching, mismatched service name rejection, profile/config validation, and full
//! `ReplayEngine` Parquet streaming.

#![cfg(feature = "iceoryx2")]

use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::port_factory::publish_subscribe::PortFactory;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use stream_replay_rs::config::{sink_from_parts, ReplayConfig, DEFAULT_SINK_URL, DEFAULT_TOPIC};
use stream_replay_rs::engine::ReplayEngine;
use stream_replay_rs::error::ReplayError;
use stream_replay_rs::sink::iceoryx2::{
    connect, create_iceoryx2_sink, AllocationStrategy, BackpressureStrategy, Iceoryx2Options,
    Iceoryx2Sink,
};
use stream_replay_rs::sink::{create_sink, Sink, SinkRecord, SinkType};
use tempfile::NamedTempFile;

mod common;

static SERVICE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a process- and test-unique iceoryx2 service name so parallel tests never collide.
fn unique_service_name(prefix: &str) -> String {
    let seq = SERVICE_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("stream_replay_test/{prefix}/{}_{seq}", std::process::id())
}

struct TestSubscriber {
    _node: Node<ipc_threadsafe::Service>,
    _service: PortFactory<ipc_threadsafe::Service, [u8], ()>,
    subscriber: Subscriber<ipc_threadsafe::Service, [u8], ()>,
}

impl TestSubscriber {
    fn open(service_name: &str, buffer_size: usize) -> Self {
        let node = NodeBuilder::new()
            .signal_handling_mode(SignalHandlingMode::Disabled)
            .create::<ipc_threadsafe::Service>()
            .expect("create test iceoryx2 node");
        let iox_name = ServiceName::new(service_name).expect("valid service name");
        let service = node
            .service_builder(&iox_name)
            .publish_subscribe::<[u8]>()
            .subscriber_max_buffer_size(buffer_size)
            .enable_safe_overflow(true)
            .open_or_create()
            .expect("open_or_create test service");
        let subscriber = service
            .subscriber_builder()
            .buffer_size(buffer_size)
            .create()
            .expect("create test subscriber");
        Self {
            _node: node,
            _service: service,
            subscriber,
        }
    }

    fn drain(&self) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        while let Some(sample) = self.subscriber.receive().expect("subscriber receive") {
            out.push(sample.payload().to_vec());
        }
        out
    }
}

#[tokio::test]
async fn zero_copy_shared_memory_replay_delivers_all_records_in_order() {
    let svc = unique_service_name("zero_copy_order");
    let sub = TestSubscriber::open(&svc, 128);
    let sink = connect(&svc, Iceoryx2Options::default()).unwrap();
    assert_eq!(sink.service_name(), svc);

    let n = 64usize;
    let when = chrono::Utc::now();
    for i in 0..n {
        let key = format!("entity-{i}");
        let payload = format!("{{\"seq\":{i},\"msg\":\"event-{i}\"}}");
        let record = SinkRecord {
            key: Some(&key),
            payload: payload.as_bytes(),
            timestamp: when,
            headers: vec![
                ("replay.run_id".into(), "run-iox2".into()),
                ("replay.seq".into(), i.to_string()),
            ],
        };
        sink.send_record(&svc, &record).await.unwrap();
    }
    sink.flush().await.unwrap();

    assert_eq!(sink.sent_records(), n as u64);
    let received = sub.drain();
    assert_eq!(received.len(), n, "subscriber must receive all N payloads");
    for (i, bytes) in received.iter().enumerate() {
        let expected = format!("{{\"seq\":{i},\"msg\":\"event-{i}\"}}");
        assert_eq!(bytes.as_slice(), expected.as_bytes());
    }
}

#[tokio::test]
async fn dynamic_slice_allocation_grows_for_larger_payloads() {
    let svc = unique_service_name("dynamic_grow");
    let sub = TestSubscriber::open(&svc, 32);
    let options = Iceoryx2Options {
        initial_max_slice_len: 16,
        allocation_strategy: AllocationStrategy::PowerOfTwo,
        max_loaned_samples: 2,
        unable_to_deliver_strategy: BackpressureStrategy::DiscardData,
        subscriber_max_buffer_size: 32,
        enable_safe_overflow: true,
    };
    let sink = Iceoryx2Sink::with_options(&svc, options).unwrap();

    let sizes = [8usize, 16, 64, 256, 1024];
    for (idx, &size) in sizes.iter().enumerate() {
        let payload = vec![b'a' + (idx as u8); size];
        sink.send(&svc, None, &payload).await.unwrap();
    }
    sink.flush().await.unwrap();
    assert_eq!(sink.sent_records(), sizes.len() as u64);

    let received = sub.drain();
    assert_eq!(received.len(), sizes.len());
    for (idx, (&size, actual)) in sizes.iter().zip(received.iter()).enumerate() {
        assert_eq!(actual.len(), size);
        assert!(actual.iter().all(|&b| b == b'a' + (idx as u8)));
    }
}

#[tokio::test]
async fn static_allocation_overflow_fails_and_latches_sink() {
    let svc = unique_service_name("static_overflow");
    let sub = TestSubscriber::open(&svc, 32);
    let options = Iceoryx2Options {
        initial_max_slice_len: 16,
        allocation_strategy: AllocationStrategy::Static,
        max_loaned_samples: 2,
        unable_to_deliver_strategy: BackpressureStrategy::DiscardData,
        subscriber_max_buffer_size: 32,
        enable_safe_overflow: true,
    };
    let sink = connect(&svc, options).unwrap();
    assert_eq!(sink.options().initial_max_slice_len, 16);

    // Payload within the static 16-byte limit succeeds
    sink.send(&svc, Some("k1"), b"0123456789ab").await.unwrap();
    assert_eq!(sink.sent_records(), 1);

    // Payload exceeding the static 16-byte limit fails loan and latches
    let oversized = vec![b'x'; 64];
    let err = sink.send(&svc, Some("k2"), &oversized).await.unwrap_err();
    assert!(matches!(err, ReplayError::Sink(_)), "{err:?}");
    assert!(
        err.to_string().contains("loan"),
        "expected loan failure diagnostic, got: {err}"
    );
    assert_eq!(sink.sent_records(), 1);

    // Subsequent sends (even small ones) and flush fail immediately due to latched failure
    assert!(sink.send(&svc, Some("k3"), b"tiny").await.is_err());
    assert!(sink.flush().await.is_err());

    let received = sub.drain();
    assert_eq!(received, vec![b"0123456789ab".to_vec()]);
}

#[tokio::test]
async fn empty_topic_falls_back_and_mismatched_service_name_is_rejected_and_latched() {
    let svc = unique_service_name("topic_matching");
    let sub = TestSubscriber::open(&svc, 32);
    let sink = Iceoryx2Sink::new(&svc).unwrap();

    // Empty topic falls back to configured service_name
    sink.send("", Some("k0"), b"{\"id\":1}").await.unwrap();
    assert_eq!(sink.sent_records(), 1);

    // Mismatched service name is rejected before shared-memory loan and latches failure
    let err = sink
        .send("other/service", None, b"{\"id\":2}")
        .await
        .unwrap_err();
    assert!(matches!(err, ReplayError::Sink(_)));
    assert_eq!(sink.sent_records(), 1);

    // Subsequent calls on the valid service name also fail because failure is latched
    assert!(sink.send(&svc, None, b"{\"id\":3}").await.is_err());
    assert!(sink.flush().await.is_err());

    let received = sub.drain();
    assert_eq!(received, vec![b"{\"id\":1}".to_vec()]);
}

#[tokio::test]
async fn config_profile_and_connect_validate_service_names_and_options() {
    // Valid sink_from_parts
    let sink = sink_from_parts("iceoryx2", "".into(), "  orders/ipc  ".into()).unwrap();
    assert_eq!(
        sink,
        SinkType::Iceoryx2 {
            service_name: "orders/ipc".into(),
        }
    );

    // URL fallback when topic is DEFAULT_TOPIC or empty
    let sink_from_url =
        sink_from_parts("iceoryx2", "orders/from_url".into(), DEFAULT_TOPIC.into()).unwrap();
    assert_eq!(
        sink_from_url,
        SinkType::Iceoryx2 {
            service_name: "orders/from_url".into(),
        }
    );
    let sink_from_url_empty_topic =
        sink_from_parts("iceoryx2", "orders/from_url".into(), "".into()).unwrap();
    assert_eq!(
        sink_from_url_empty_topic,
        SinkType::Iceoryx2 {
            service_name: "orders/from_url".into(),
        }
    );

    // Empty or whitespace-only or reserved iox2:// service names are rejected
    for invalid in ["", "   ", "iox2://reserved"] {
        let err = sink_from_parts("iceoryx2", DEFAULT_SINK_URL.into(), invalid.into()).unwrap_err();
        assert!(
            matches!(err, ReplayError::Config(_)),
            "expected Config error for '{invalid}', got {err:?}"
        );

        assert!(ReplayConfig::builder()
            .file_path("events.csv".into())
            .iceoryx2_sink(invalid)
            .build()
            .is_err());

        assert!(connect(invalid, Iceoryx2Options::default()).is_err());
        assert!(create_iceoryx2_sink(invalid).await.is_err());
    }

    // Builder trims surrounding whitespace via both iceoryx2_sink and sink()
    let cfg = ReplayConfig::builder()
        .file_path("events.csv".into())
        .iceoryx2_sink("  orders/trimmed  ")
        .build()
        .unwrap();
    assert_eq!(
        cfg.sink,
        SinkType::Iceoryx2 {
            service_name: "orders/trimmed".into(),
        }
    );
    let cfg_via_sink = ReplayConfig::builder()
        .file_path("events.csv".into())
        .sink(SinkType::Iceoryx2 {
            service_name: "  orders/trimmed_sink  ".into(),
        })
        .build()
        .unwrap();
    assert_eq!(
        cfg_via_sink.sink,
        SinkType::Iceoryx2 {
            service_name: "orders/trimmed_sink".into(),
        }
    );

    // Invalid Iceoryx2Options are rejected at connect
    let svc = unique_service_name("bad_opts");
    assert!(connect(
        &svc,
        Iceoryx2Options {
            initial_max_slice_len: 0,
            ..Default::default()
        }
    )
    .is_err());
    assert!(connect(
        &svc,
        Iceoryx2Options {
            max_loaned_samples: 0,
            ..Default::default()
        }
    )
    .is_err());
    assert!(connect(
        &svc,
        Iceoryx2Options {
            subscriber_max_buffer_size: 0,
            ..Default::default()
        }
    )
    .is_err());

    // Sectioned TOML profile with service_name
    let mut sectioned = NamedTempFile::new().unwrap();
    write!(
        sectioned,
        r#"
[dataset]
input_path = "events.csv"

[sink]
default_type = "iceoryx2"
service_name = "orders/profile_svc"
"#
    )
    .unwrap();
    let cfg = ReplayConfig::from_profile_file(sectioned.path()).unwrap();
    assert_eq!(
        cfg.sink,
        SinkType::Iceoryx2 {
            service_name: "orders/profile_svc".into(),
        }
    );

    // Sectioned TOML profile with destination fallback
    let mut dest_profile = NamedTempFile::new().unwrap();
    write!(
        dest_profile,
        r#"
[dataset]
input_path = "events.csv"

[sink]
default_type = "iceoryx2"
destination = "orders/dest_svc"
"#
    )
    .unwrap();
    let cfg = ReplayConfig::from_profile_file(dest_profile.path()).unwrap();
    assert_eq!(
        cfg.sink,
        SinkType::Iceoryx2 {
            service_name: "orders/dest_svc".into(),
        }
    );

    // Flat TOML profile with service_name
    let mut flat = NamedTempFile::new().unwrap();
    write!(
        flat,
        r#"
input_file_path = "events.csv"
sink = "iceoryx2"
service_name = "orders/flat_svc"
"#
    )
    .unwrap();
    let cfg = ReplayConfig::from_profile_file(flat.path()).unwrap();
    assert_eq!(
        cfg.sink,
        SinkType::Iceoryx2 {
            service_name: "orders/flat_svc".into(),
        }
    );

    // Sectioned TOML profile with empty service_name is rejected
    let mut empty_svc_profile = NamedTempFile::new().unwrap();
    write!(
        empty_svc_profile,
        r#"
[dataset]
input_path = "events.csv"

[sink]
default_type = "iceoryx2"
service_name = "   "
"#
    )
    .unwrap();
    assert!(ReplayConfig::from_profile_file(empty_svc_profile.path()).is_err());
}

#[tokio::test]
async fn incompatible_existing_service_type_fails_connect() {
    let svc = unique_service_name("incompatible_type");
    let node = NodeBuilder::new()
        .signal_handling_mode(SignalHandlingMode::Disabled)
        .create::<ipc_threadsafe::Service>()
        .unwrap();
    let iox_name = ServiceName::new(&svc).unwrap();
    // Create a fixed-size u64 service with the same name so [u8] is incompatible
    let _existing_u64_service = node
        .service_builder(&iox_name)
        .publish_subscribe::<u64>()
        .create()
        .unwrap();

    assert!(connect(&svc, Iceoryx2Options::default()).is_err());
    assert!(create_sink(&SinkType::Iceoryx2 { service_name: svc })
        .await
        .is_err());
}

#[tokio::test]
async fn full_replay_engine_parquet_run_streams_all_records_over_shared_memory() {
    let svc = unique_service_name("engine_parquet");
    let sub = TestSubscriber::open(&svc, 128);

    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("events.parquet");
    common::write_parquet_events(&data, 50, 1);

    let config = ReplayConfig::builder()
        .file_path(data)
        .timestamp_column("event_ts")
        .key_column("id")
        .speed_multiplier(0.0)
        .progress(false)
        .iceoryx2_sink(&svc)
        .build()
        .unwrap();

    let mut engine = ReplayEngine::new(config).await.unwrap();
    let stats = engine.run().await.unwrap();
    assert_eq!(stats.total_records, 50);

    let received = sub.drain();
    assert_eq!(
        received.len(),
        50,
        "all 50 Parquet records must be received over shared memory"
    );

    for (i, payload) in received.iter().enumerate() {
        let parsed: serde_json::Value =
            serde_json::from_slice(payload).expect("valid JSON payload");
        assert_eq!(
            parsed.get("id").and_then(|v| v.as_str()),
            Some(format!("evt_{i:04}").as_str())
        );
        assert_eq!(parsed.get("value").and_then(|v| v.as_i64()), Some(i as i64));
        assert!(parsed.get("event_ts").is_some());
    }
}
