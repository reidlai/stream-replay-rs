use stream_replay_rs::sink::stdout::StdoutSink;
use stream_replay_rs::sink::{create_sink, Sink, SinkType};

#[tokio::test]
async fn test_stdout_sink_recording() {
    let sink = StdoutSink::new_silent("test-topic");
    sink.send("test-topic", Some("key-1"), b"{\"hello\": \"world\"}")
        .await
        .unwrap();
    sink.send("test-topic", None, b"{\"event\": 2}")
        .await
        .unwrap();

    let recorded = sink.get_recorded();
    assert_eq!(recorded.len(), 2);
    assert_eq!(recorded[0].0, "test-topic");
    assert_eq!(recorded[0].1.as_deref(), Some("key-1"));
    assert_eq!(recorded[0].2, b"{\"hello\": \"world\"}");
    assert_eq!(recorded[1].1, None);
}

#[tokio::test]
async fn test_sink_factory_creation() {
    // Stdout
    let s_stdout = create_sink(&SinkType::Stdout {
        topic: "my-topic".into(),
    })
    .await;
    assert!(s_stdout.is_ok());

    // Kafka
    let s_kafka = create_sink(&SinkType::Kafka {
        broker_url: "localhost:9092".into(),
        topic: "orders".into(),
    })
    .await;
    assert!(s_kafka.is_ok());

    // NATS
    let s_nats = create_sink(&SinkType::Nats {
        url: "localhost:4222".into(),
        topic: "orders.jetstream".into(),
    })
    .await;
    assert!(s_nats.is_ok());

    // Fluvio
    let s_fluvio = create_sink(&SinkType::Fluvio {
        url: "localhost:9003".into(),
        topic: "fluvio-orders".into(),
    })
    .await;
    assert!(s_fluvio.is_ok());

    // iceoryx2
    let s_iceoryx2 = create_sink(&SinkType::Iceoryx2 {
        service_name: "shared_mem_orders".into(),
    })
    .await;
    assert!(s_iceoryx2.is_ok());
}

