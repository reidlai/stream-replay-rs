use chrono::{DateTime, Utc};
use std::io::Write;
use std::sync::Arc;
use stream_replay_rs::config::ReplayConfig;
use stream_replay_rs::engine::ReplayEngine;
use stream_replay_rs::sink::stdout::StdoutSink;
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_engine_csv_replay_end_to_end() {
    let mut tmp = NamedTempFile::new().unwrap();
    writeln!(tmp, "order_id,timestamp,value").unwrap();
    writeln!(tmp, "ord_A,2017-01-01 10:00:00,10.0").unwrap();
    writeln!(tmp, "ord_B,2017-01-01 10:00:05,20.0").unwrap();
    writeln!(tmp, "ord_C,2017-01-01 10:00:10,30.0").unwrap();
    tmp.flush().unwrap();

    let sink = Arc::new(StdoutSink::new_silent("orders/intake"));

    let config = ReplayConfig::builder()
        .file_path(tmp.path().to_path_buf())
        .timestamp_column("timestamp")
        .key_column("order_id")
        .speed_multiplier(100.0) // high speed for fast test
        .progress(false)
        .build()
        .unwrap();

    let mut engine = ReplayEngine::new(config)
        .await
        .unwrap()
        .with_sink(sink.clone());
    let stats = engine.run().await.unwrap();

    assert_eq!(stats.total_records, 3);
    let recorded = sink.get_recorded();
    assert_eq!(recorded.len(), 3);

    // Verify key
    assert_eq!(recorded[0].1.as_deref(), Some("ord_A"));
    assert_eq!(recorded[1].1.as_deref(), Some("ord_B"));
    assert_eq!(recorded[2].1.as_deref(), Some("ord_C"));

    // Verify timestamps are shifted to live UTC time and monotonic
    let json0: serde_json::Value = serde_json::from_slice(&recorded[0].2).unwrap();
    let json1: serde_json::Value = serde_json::from_slice(&recorded[1].2).unwrap();
    let json2: serde_json::Value = serde_json::from_slice(&recorded[2].2).unwrap();

    let ts0: DateTime<Utc> = json0["timestamp"].as_str().unwrap().parse().unwrap();
    let ts1: DateTime<Utc> = json1["timestamp"].as_str().unwrap().parse().unwrap();
    let ts2: DateTime<Utc> = json2["timestamp"].as_str().unwrap().parse().unwrap();

    assert!(ts1 > ts0);
    assert!(ts2 > ts1);
}

#[tokio::test]
async fn test_engine_burst_mode() {
    let mut tmp = NamedTempFile::new().unwrap();
    writeln!(tmp, "order_id,timestamp,value").unwrap();
    for i in 0..20 {
        writeln!(tmp, "ord_{i:03},2017-01-01 10:{i:02}:00,10.0").unwrap();
    }
    tmp.flush().unwrap();

    let sink = Arc::new(StdoutSink::new_silent("orders/burst"));

    let config = ReplayConfig::builder()
        .file_path(tmp.path().to_path_buf())
        .timestamp_column("timestamp")
        .key_column("order_id")
        .burst(5) // Only emit 5 in burst
        .progress(false)
        .build()
        .unwrap();

    let mut engine = ReplayEngine::new(config)
        .await
        .unwrap()
        .with_sink(sink.clone());
    let stats = engine.run().await.unwrap();

    assert_eq!(stats.total_records, 5);
    assert_eq!(sink.get_recorded().len(), 5);
}

#[tokio::test]
async fn test_profile_toml_loading() {
    let mut tmp_profile = NamedTempFile::new().unwrap();
    let mut tmp_data = NamedTempFile::new().unwrap();
    writeln!(tmp_data, "id,timestamp\n1,2026-01-01 00:00:00").unwrap();
    tmp_data.flush().unwrap();

    let data_path_escaped = tmp_data.path().to_str().unwrap().replace('\\', "\\\\");

    writeln!(
        tmp_profile,
        r#"
input_file_path = "{data_path_escaped}"
timestamp_column = "timestamp"
sink = "stdout"
topic = "profile_topic"
speed = 5.0
loop_playback = true
advance_loop_timestamps = true
loop_id_salt = true
auth_url = "https://localhost:8443"
service_account = "svc_stream_replay"
service_secret = "m2m_secret_val"
auth_token = "m2m_token_val"
"#
    )
    .unwrap();
    tmp_profile.flush().unwrap();

    let config = ReplayConfig::from_profile_file(tmp_profile.path()).unwrap();
    assert_eq!(config.timestamp_column, "timestamp");
    assert_eq!(config.speed_multiplier, 5.0);
    assert!(config.loop_playback);
    assert!(config.advance_loop_timestamps);
    assert!(config.loop_id_salt);
    assert_eq!(config.auth_url.as_deref(), Some("https://localhost:8443"));
    assert_eq!(config.service_account.as_deref(), Some("svc_stream_replay"));
    assert_eq!(config.service_secret.as_deref(), Some("m2m_secret_val"));
    assert_eq!(config.auth_token.as_deref(), Some("m2m_token_val"));
}

#[tokio::test]
async fn test_engine_loop_playback_salting_and_advance() {
    let mut tmp = NamedTempFile::new().unwrap();
    writeln!(tmp, "order_id,timestamp,value").unwrap();
    writeln!(tmp, "ord_100,2026-10-30 08:00:00,10.0").unwrap();
    writeln!(tmp, "ord_200,2026-10-30 08:05:00,20.0").unwrap();
    tmp.flush().unwrap();

    let sink = Arc::new(StdoutSink::new_silent("orders/loop_test"));

    let config = ReplayConfig::builder()
        .file_path(tmp.path().to_path_buf())
        .timestamp_column("timestamp")
        .key_column("order_id")
        .loop_playback(true)
        .advance_loop_timestamps(true)
        .loop_id_salt(true)
        .burst(4) // 2 records * 2 loops = 4 records
        .speed_multiplier(100.0)
        .progress(false)
        .build()
        .unwrap();

    let mut engine = ReplayEngine::new(config)
        .await
        .unwrap()
        .with_sink(sink.clone());
    let stats = engine.run().await.unwrap();

    assert_eq!(stats.total_records, 4);
    let recorded = sink.get_recorded();
    assert_eq!(recorded.len(), 4);

    // Loop 0 keys: original
    assert_eq!(recorded[0].1.as_deref(), Some("ord_100"));
    assert_eq!(recorded[1].1.as_deref(), Some("ord_200"));

    // Loop 1 keys: salted with _L1
    assert_eq!(recorded[2].1.as_deref(), Some("ord_100_L1"));
    assert_eq!(recorded[3].1.as_deref(), Some("ord_200_L1"));

    // Check payload JSON keys and timestamps
    let p0: serde_json::Value = serde_json::from_slice(&recorded[0].2).unwrap();
    let p2: serde_json::Value = serde_json::from_slice(&recorded[2].2).unwrap();

    assert_eq!(p0["order_id"], "ord_100");
    assert_eq!(p2["order_id"], "ord_100_L1");

    let t0: DateTime<Utc> = p0["timestamp"].as_str().unwrap().parse().unwrap();
    let t2: DateTime<Utc> = p2["timestamp"].as_str().unwrap().parse().unwrap();
    assert!(t2 > t0, "Loop 1 timestamp must advance beyond Loop 0");
}

#[tokio::test]
async fn test_engine_parquet_replay() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::fs::File;

    let tmp = NamedTempFile::new().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("item_id", DataType::Utf8, false),
        Field::new("created_at", DataType::Utf8, false),
        Field::new("qty", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["item_1", "item_2"])),
            Arc::new(StringArray::from(vec![
                "2026-10-30T12:00:00Z",
                "2026-10-30T12:01:00Z",
            ])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .unwrap();

    let file = File::create(tmp.path()).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let sink = Arc::new(StdoutSink::new_silent("items/stream"));

    let config = ReplayConfig::builder()
        .file_path(tmp.path().to_path_buf())
        .file_format("parquet")
        .timestamp_column("created_at")
        .key_column("item_id")
        .burst(2)
        .progress(false)
        .build()
        .unwrap();

    let mut engine = ReplayEngine::new(config)
        .await
        .unwrap()
        .with_sink(sink.clone());
    let stats = engine.run().await.unwrap();

    assert_eq!(stats.total_records, 2);
    let recorded = sink.get_recorded();
    assert_eq!(recorded.len(), 2);
    assert_eq!(recorded[0].1.as_deref(), Some("item_1"));
    assert_eq!(recorded[1].1.as_deref(), Some("item_2"));
}

#[tokio::test]
async fn test_engine_rate_pacing() {
    let mut tmp = NamedTempFile::new().unwrap();
    writeln!(
        tmp,
        "id,timestamp\n1,2026-10-30 00:00:00\n2,2026-10-30 00:00:01"
    )
    .unwrap();
    tmp.flush().unwrap();

    let sink = Arc::new(StdoutSink::new_silent("rate/test"));

    let config = ReplayConfig::builder()
        .file_path(tmp.path().to_path_buf())
        .timestamp_column("timestamp")
        .key_column("id")
        .rate(100) // 100 msg/sec -> 10ms pacing
        .progress(false)
        .build()
        .unwrap();

    let mut engine = ReplayEngine::new(config)
        .await
        .unwrap()
        .with_sink(sink.clone());
    let stats = engine.run().await.unwrap();

    assert_eq!(stats.total_records, 2);
}
