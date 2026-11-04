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
}

/// There is no simulated mode: a Kafka sink without a reachable endpoint is an error.
#[tokio::test]
async fn test_kafka_sink_fails_without_an_endpoint() {
    // Bind and drop a listener to get a local port that nothing listens on.
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let result = create_sink(&SinkType::Kafka {
        broker_url: format!("127.0.0.1:{port}"),
        topic: "orders".into(),
    })
    .await;
    assert!(
        result.is_err(),
        "an unreachable endpoint must fail, not simulate"
    );
}
