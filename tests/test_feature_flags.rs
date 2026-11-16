use stream_replay_rs::config::{sink_from_parts, sink_parts, ReplayConfig};
use stream_replay_rs::error::ReplayError;
use stream_replay_rs::sink::SinkType;

#[test]
fn test_stdout_sink_always_available() {
    let sink = sink_from_parts("stdout", "localhost:9092".into(), "events".into()).unwrap();
    assert_eq!(
        sink,
        SinkType::Stdout {
            topic: "events".into()
        }
    );
    assert_eq!(sink.destination(), "events");

    let (kind, url, topic) = sink_parts(&sink);
    assert_eq!(kind, "stdout");
    assert_eq!(topic, "events");
    let _ = url;
}

#[test]
fn test_unknown_sink_returns_helpful_error() {
    let err = sink_from_parts("redis", "localhost:6379".into(), "events".into()).unwrap_err();
    match err {
        ReplayError::Config(msg) => {
            assert!(
                msg.contains("Unknown sink 'redis'"),
                "expected unknown sink diagnostic, got: {msg}"
            );
            assert!(
                msg.contains("Supported sinks:"),
                "expected supported sinks listing, got: {msg}"
            );
        }
        other => panic!("expected ReplayError::Config, got {other:?}"),
    }
}

#[cfg(not(feature = "nats"))]
#[test]
fn test_uncompiled_nats_sink_diagnostics() {
    let err = sink_from_parts("nats", "localhost:4222".into(), "orders".into()).unwrap_err();
    match err {
        ReplayError::Config(msg) => {
            assert_eq!(
                msg,
                "NATS sink requested, but stream-replay-rs was compiled without '--features nats'"
            );
        }
        other => panic!("expected ReplayError::Config, got {other:?}"),
    }
}

#[cfg(not(feature = "fluvio"))]
#[test]
fn test_uncompiled_fluvio_sink_diagnostics() {
    let err = sink_from_parts("fluvio", "localhost:9003".into(), "orders".into()).unwrap_err();
    match err {
        ReplayError::Config(msg) => {
            assert_eq!(
                msg,
                "Fluvio sink requested, but stream-replay-rs was compiled without '--features fluvio'"
            );
        }
        other => panic!("expected ReplayError::Config, got {other:?}"),
    }
}

#[cfg(not(feature = "iceoryx2"))]
#[test]
fn test_uncompiled_iceoryx2_sink_diagnostics() {
    let err = sink_from_parts("iceoryx2", "".into(), "service_channel".into()).unwrap_err();
    match err {
        ReplayError::Config(msg) => {
            assert_eq!(
                msg,
                "iceoryx2 sink requested, but stream-replay-rs was compiled without '--features iceoryx2'"
            );
        }
        other => panic!("expected ReplayError::Config, got {other:?}"),
    }
}

#[cfg(not(feature = "kafka"))]
#[test]
fn test_uncompiled_kafka_sink_diagnostics() {
    let err = sink_from_parts("kafka", "localhost:9092".into(), "orders".into()).unwrap_err();
    match err {
        ReplayError::Config(msg) => {
            assert_eq!(
                msg,
                "Kafka sink requested, but stream-replay-rs was compiled without '--features kafka'"
            );
        }
        other => panic!("expected ReplayError::Config, got {other:?}"),
    }
}

#[cfg(feature = "kafka")]
#[test]
fn test_compiled_kafka_sink_dispatch() {
    let sink = sink_from_parts("kafka", "broker:9092".into(), "events".into()).unwrap();
    assert_eq!(
        sink,
        SinkType::Kafka {
            broker_url: "broker:9092".into(),
            topic: "events".into(),
        }
    );
    assert_eq!(sink.destination(), "events");

    let (kind, url, topic) = sink_parts(&sink);
    assert_eq!(kind, "kafka");
    assert_eq!(url, "broker:9092");
    assert_eq!(topic, "events");

    // Test iggy alias
    let iggy = sink_from_parts("iggy", "localhost:9092".into(), "stream".into()).unwrap();
    assert_eq!(
        iggy,
        SinkType::Kafka {
            broker_url: "localhost:9092".into(),
            topic: "stream".into(),
        }
    );
}

#[cfg(feature = "nats")]
#[test]
fn test_compiled_nats_sink_dispatch() {
    let sink = sink_from_parts("nats", "nats://127.0.0.1:4222".into(), "telemetry".into()).unwrap();
    assert_eq!(
        sink,
        SinkType::Nats {
            url: "nats://127.0.0.1:4222".into(),
            subject: "telemetry".into(),
        }
    );
    assert_eq!(sink.destination(), "telemetry");

    let (kind, url, topic) = sink_parts(&sink);
    assert_eq!(kind, "nats");
    assert_eq!(url, "nats://127.0.0.1:4222");
    assert_eq!(topic, "telemetry");
}

#[cfg(feature = "fluvio")]
#[test]
fn test_compiled_fluvio_sink_dispatch() {
    let sink = sink_from_parts("fluvio", "127.0.0.1:9003".into(), "stream".into()).unwrap();
    assert_eq!(
        sink,
        SinkType::Fluvio {
            endpoint: "127.0.0.1:9003".into(),
            topic: "stream".into(),
        }
    );
    assert_eq!(sink.destination(), "stream");

    let (kind, url, topic) = sink_parts(&sink);
    assert_eq!(kind, "fluvio");
    assert_eq!(url, "127.0.0.1:9003");
    assert_eq!(topic, "stream");
}

#[cfg(feature = "iceoryx2")]
#[test]
fn test_compiled_iceoryx2_sink_dispatch() {
    let sink = sink_from_parts("iceoryx2", "".into(), "service_a".into()).unwrap();
    assert_eq!(
        sink,
        SinkType::Iceoryx2 {
            service_name: "service_a".into(),
        }
    );
    assert_eq!(sink.destination(), "service_a");

    let (kind, _, topic) = sink_parts(&sink);
    assert_eq!(kind, "iceoryx2");
    assert_eq!(topic, "service_a");
}

#[test]
fn test_replay_config_builder_sink_methods() {
    let cfg = ReplayConfig::builder()
        .file_path("events.csv".into())
        .stdout_sink("local-topic")
        .build()
        .unwrap();
    assert_eq!(
        cfg.sink,
        SinkType::Stdout {
            topic: "local-topic".into()
        }
    );

    #[cfg(feature = "kafka")]
    {
        let cfg = ReplayConfig::builder()
            .file_path("events.csv".into())
            .kafka_sink("broker:9092", "kafka-topic")
            .build()
            .unwrap();
        assert_eq!(
            cfg.sink,
            SinkType::Kafka {
                broker_url: "broker:9092".into(),
                topic: "kafka-topic".into(),
            }
        );
    }

    #[cfg(feature = "nats")]
    {
        let cfg = ReplayConfig::builder()
            .file_path("events.csv".into())
            .nats_sink("localhost:4222", "nats-subject")
            .build()
            .unwrap();
        assert_eq!(
            cfg.sink,
            SinkType::Nats {
                url: "localhost:4222".into(),
                subject: "nats-subject".into(),
            }
        );
    }

    #[cfg(feature = "fluvio")]
    {
        let cfg = ReplayConfig::builder()
            .file_path("events.csv".into())
            .fluvio_sink("localhost:9003", "fluvio-topic")
            .build()
            .unwrap();
        assert_eq!(
            cfg.sink,
            SinkType::Fluvio {
                endpoint: "localhost:9003".into(),
                topic: "fluvio-topic".into(),
            }
        );
    }

    #[cfg(feature = "iceoryx2")]
    {
        let cfg = ReplayConfig::builder()
            .file_path("events.csv".into())
            .iceoryx2_sink("iceoryx2-service")
            .build()
            .unwrap();
        assert_eq!(
            cfg.sink,
            SinkType::Iceoryx2 {
                service_name: "iceoryx2-service".into(),
            }
        );
    }
}
