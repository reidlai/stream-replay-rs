use std::sync::Arc;
use stream_replay_rs::auth::DEFAULT_SERVICE_ACCOUNT;
use stream_replay_rs::config::ReplayConfig;
use stream_replay_rs::data_generator::generate_olist_orders_parquet;
use stream_replay_rs::engine::ReplayEngine;
use stream_replay_rs::sink::kafka::KafkaSink;
use stream_replay_rs::sink::stdout::StdoutSink;
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_olist_parquet_generation_and_schema() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    // Generate 50 realistic Olist orders
    generate_olist_orders_parquet(&path, 50).unwrap();
    assert!(path.exists());
    assert!(path.metadata().unwrap().len() > 0);

    // Verify reader reads schema columns
    let config = ReplayConfig::builder()
        .file_path(path.clone())
        .file_format("parquet")
        .timestamp_column("order_purchase_timestamp")
        .key_column("order_id")
        .progress(false)
        .burst(5)
        .build()
        .unwrap();

    let sink = Arc::new(StdoutSink::new_silent("orders/intake"));
    let mut engine = ReplayEngine::new(config.clone()).await.unwrap().with_sink(sink.clone());
    let stats = engine.run().await.unwrap();

    assert_eq!(stats.total_records, 5);
    let recorded = sink.get_recorded();
    assert_eq!(recorded.len(), 5);

    // Verify key starts with olist_ord_
    assert!(recorded[0].1.as_ref().unwrap().starts_with("olist_ord_"));

    // Verify payload JSON fields
    let p0: serde_json::Value = serde_json::from_slice(&recorded[0].2).unwrap();
    assert!(p0.get("order_id").is_some());
    assert!(p0.get("customer_id").is_some());
    assert!(p0.get("order_status").is_some());
    assert!(p0.get("order_purchase_timestamp").is_some());
}

#[tokio::test]
async fn test_olist_streaming_into_kafka_iggy_with_m2m_auth() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    generate_olist_orders_parquet(&path, 30).unwrap();

    let kafka_sink = Arc::new(KafkaSink::new("localhost:9092", "orders/intake"));

    let config = ReplayConfig::builder()
        .file_path(path)
        .file_format("parquet")
        .timestamp_column("order_purchase_timestamp")
        .key_column("order_id")
        .auth(
            None,
            Some(DEFAULT_SERVICE_ACCOUNT.to_string()),
            None,
            Some("test_token_stream_replay".to_string()),
        )
        .progress(false)
        .burst(30)
        .build()
        .unwrap();

    let mut engine = ReplayEngine::new(config).await.unwrap().with_sink(kafka_sink.clone());
    let stats = engine.run().await.unwrap();

    assert_eq!(stats.total_records, 30);
    // Verify all 30 records were confirmed with partition offset tracking
    assert_eq!(kafka_sink.last_offset(), 30);
    assert_eq!(engine.authenticator().service_account, "svc_stream_replay");
    assert!(engine.authenticator().is_authenticated);
}

#[tokio::test]
async fn test_olist_loop_salting_into_kafka_sink() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    generate_olist_orders_parquet(&path, 10).unwrap();

    let sink = Arc::new(StdoutSink::new_silent("orders/intake"));

    let config = ReplayConfig::builder()
        .file_path(path)
        .file_format("parquet")
        .timestamp_column("order_purchase_timestamp")
        .key_column("order_id")
        .loop_playback(true)
        .advance_loop_timestamps(true)
        .loop_id_salt(true)
        .burst(20) // 10 records * 2 loops = 20 records
        .speed_multiplier(100.0)
        .progress(false)
        .build()
        .unwrap();

    let mut engine = ReplayEngine::new(config.clone()).await.unwrap().with_sink(sink.clone());
    let stats = engine.run().await.unwrap();

    assert_eq!(stats.total_records, 20);
    let recorded = sink.get_recorded();
    assert_eq!(recorded.len(), 20);

    // Loop 0: unsalted
    assert!(!recorded[0].1.as_ref().unwrap().contains("_L"));
    // Loop 1: salted with _L1
    assert!(recorded[10].1.as_ref().unwrap().contains("_L1"));

    let p10: serde_json::Value = serde_json::from_slice(&recorded[10].2).unwrap();
    assert!(p10["order_id"].as_str().unwrap().contains("_L1"));

    // Also verify KafkaSink partition offset advancement across multi-loop replay
    let kafka_sink = Arc::new(KafkaSink::new("localhost:9092", "orders/intake"));
    let mut engine_kafka = ReplayEngine::new(config).await.unwrap().with_sink(kafka_sink.clone());
    let stats_kafka = engine_kafka.run().await.unwrap();
    assert_eq!(stats_kafka.total_records, 20);
    assert_eq!(kafka_sink.last_offset(), 20);
}

#[tokio::test]
async fn test_packaged_olist_orders_parquet_replay() {
    use std::path::Path;

    let path = Path::new("data/olist_orders.parquet");
    if !path.exists() {
        // Generate packaged dataset if running from test harness before first CLI launch
        generate_olist_orders_parquet(path, 50).unwrap();
    }
    assert!(path.exists(), "Flagship dataset data/olist_orders.parquet must exist");

    let kafka_sink = Arc::new(KafkaSink::new("localhost:9092", "orders/intake"));

    let config = ReplayConfig::builder()
        .file_path(path.to_path_buf())
        .file_format("parquet")
        .timestamp_column("order_purchase_timestamp")
        .key_column("order_id")
        .burst(15)
        .progress(false)
        .build()
        .unwrap();

    let mut engine = ReplayEngine::new(config).await.unwrap().with_sink(kafka_sink.clone());
    let stats = engine.run().await.unwrap();

    assert_eq!(stats.total_records, 15);
    assert_eq!(kafka_sink.last_offset(), 15);
    assert!(engine.authenticator().is_authenticated);
}
