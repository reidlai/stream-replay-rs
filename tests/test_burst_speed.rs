use std::io::Write;
use std::sync::Arc;
use stream_replay_rs::config::{parse_speed_preset, ReplayConfig};
use stream_replay_rs::data_generator::generate_olist_orders_parquet;
use stream_replay_rs::engine::ReplayEngine;
use stream_replay_rs::error::ReplayError;
use stream_replay_rs::sink::stdout::StdoutSink;
use tempfile::NamedTempFile;

#[test]
fn test_default_speed_is_10x() {
    let default_cfg = ReplayConfig::default();
    assert_eq!(default_cfg.speed_multiplier, 10.0);
}

#[test]
fn test_speed_preset_parsing() {
    // Standard multipliers & presets
    assert_eq!(parse_speed_preset("1x").unwrap(), 1.0);
    assert_eq!(parse_speed_preset("1").unwrap(), 1.0);
    assert_eq!(parse_speed_preset("10x").unwrap(), 10.0);
    assert_eq!(parse_speed_preset("10").unwrap(), 10.0);
    assert_eq!(parse_speed_preset("50x").unwrap(), 50.0);
    assert_eq!(parse_speed_preset("50").unwrap(), 50.0);
    assert_eq!(parse_speed_preset("max").unwrap(), 0.0);
    assert_eq!(parse_speed_preset("unthrottled").unwrap(), 0.0);
    assert_eq!(parse_speed_preset("0").unwrap(), 0.0);
    assert_eq!(parse_speed_preset("0x").unwrap(), 0.0);
    assert_eq!(parse_speed_preset("2.5").unwrap(), 2.5);
    assert_eq!(parse_speed_preset("2.5x").unwrap(), 2.5);
    assert_eq!(parse_speed_preset("100x").unwrap(), 100.0);

    // Case-insensitivity & whitespace trimming
    assert_eq!(parse_speed_preset(" 10X ").unwrap(), 10.0);
    assert_eq!(parse_speed_preset("  MAX  ").unwrap(), 0.0);
    assert_eq!(parse_speed_preset("  Unthrottled ").unwrap(), 0.0);

    // Negative values normalize to unthrottled 0.0
    assert_eq!(parse_speed_preset("-1.0").unwrap(), 0.0);
    assert_eq!(parse_speed_preset("-5x").unwrap(), 0.0);

    // Invalid formats & non-finite numbers must error out cleanly
    let err_invalid = parse_speed_preset("invalid_preset").unwrap_err();
    assert!(matches!(err_invalid, ReplayError::Config(_)));
    assert!(parse_speed_preset("10xx").is_err());
    assert!(parse_speed_preset("nan").is_err());
    assert!(parse_speed_preset("inf").is_err());
    assert!(parse_speed_preset("-inf").is_err());
}

#[test]
fn test_profile_toml_speed_deserialization_and_default() {
    let mut tmp = NamedTempFile::new().unwrap();
    write!(
        tmp,
        r#"
input_file_path = "data/olist_orders.parquet"
speed = "50x"
"#
    )
    .unwrap();

    let cfg = ReplayConfig::from_profile_file(tmp.path()).unwrap();
    assert_eq!(cfg.speed_multiplier, 50.0);

    // Profile omitting speed must calibrate default to 10.0
    let mut tmp_default = NamedTempFile::new().unwrap();
    write!(
        tmp_default,
        r#"
input_file_path = "data/olist_orders.parquet"
"#
    )
    .unwrap();

    let cfg_default = ReplayConfig::from_profile_file(tmp_default.path()).unwrap();
    assert_eq!(cfg_default.speed_multiplier, 10.0);

    // Numeric float in TOML
    let mut tmp_numeric = NamedTempFile::new().unwrap();
    write!(
        tmp_numeric,
        r#"
input_file_path = "data/olist_orders.parquet"
speed = 25.5
"#
    )
    .unwrap();

    let cfg_numeric = ReplayConfig::from_profile_file(tmp_numeric.path()).unwrap();
    assert_eq!(cfg_numeric.speed_multiplier, 25.5);
}

#[tokio::test]
async fn test_500_order_micro_burst_under_one_second() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    // Generate 500 realistic Olist orders
    generate_olist_orders_parquet(&path, 500).unwrap();

    let sink = Arc::new(StdoutSink::new_silent("orders/burst"));

    let config = ReplayConfig::builder()
        .file_path(path)
        .file_format("parquet")
        .timestamp_column("order_purchase_timestamp")
        .key_column("order_id")
        .burst(500) // Micro-burst injection
        .progress(false)
        .build()
        .unwrap();

    let mut engine = ReplayEngine::new(config)
        .await
        .unwrap()
        .with_sink(sink.clone());
    let stats = engine.run().await.unwrap();

    assert_eq!(stats.total_records, 500);
    assert_eq!(stats.partition_offsets, 500);
    assert_eq!(sink.get_recorded().len(), 500);

    // Verify micro-burst throughput calculation
    assert!(
        stats.records_per_sec >= 500.0,
        "Burst throughput was {:.1} msgs/s (expected >= 500 msgs/s)",
        stats.records_per_sec
    );
}

#[tokio::test]
async fn test_burst_zero_boundary() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    generate_olist_orders_parquet(&path, 10).unwrap();

    let sink = Arc::new(StdoutSink::new_silent("orders/burst_zero"));

    let config = ReplayConfig::builder()
        .file_path(path)
        .file_format("parquet")
        .timestamp_column("order_purchase_timestamp")
        .key_column("order_id")
        .burst(0) // 0 records
        .progress(false)
        .build()
        .unwrap();

    let mut engine = ReplayEngine::new(config)
        .await
        .unwrap()
        .with_sink(sink.clone());
    let stats = engine.run().await.unwrap();

    assert_eq!(stats.total_records, 0);
    assert_eq!(stats.partition_offsets, 0);
    assert_eq!(sink.get_recorded().len(), 0);
}

#[tokio::test]
async fn test_engine_replay_across_speed_multipliers() {
    let mut tmp = NamedTempFile::new().unwrap();
    writeln!(tmp, "order_id,order_purchase_timestamp,data").unwrap();
    for i in 0..10 {
        writeln!(tmp, "ord_{i},2026-01-01T00:00:00.{:03}Z,payload_{i}", i * 5).unwrap();
    }
    let path = tmp.path().to_path_buf();

    // 1. Unthrottled / max speed mode (speed_multiplier = 0.0)
    let sink_max = Arc::new(StdoutSink::new_silent("orders/speed_max"));
    let config_max = ReplayConfig::builder()
        .file_path(path.clone())
        .file_format("csv")
        .timestamp_column("order_purchase_timestamp")
        .key_column("order_id")
        .speed_multiplier(0.0) // max
        .progress(false)
        .build()
        .unwrap();

    let mut engine_max = ReplayEngine::new(config_max)
        .await
        .unwrap()
        .with_sink(sink_max.clone());
    let stats_max = engine_max.run().await.unwrap();
    assert_eq!(stats_max.total_records, 10);
    assert_eq!(stats_max.partition_offsets, 10);
    assert!(stats_max.records_per_sec > 0.0);

    // 2. High-speed multiplier mode (speed_multiplier = 50.0)
    let sink_50x = Arc::new(StdoutSink::new_silent("orders/speed_50x"));
    let config_50x = ReplayConfig::builder()
        .file_path(path.clone())
        .file_format("csv")
        .timestamp_column("order_purchase_timestamp")
        .key_column("order_id")
        .speed_multiplier(50.0)
        .progress(false)
        .build()
        .unwrap();

    let mut engine_50x = ReplayEngine::new(config_50x)
        .await
        .unwrap()
        .with_sink(sink_50x.clone());
    let stats_50x = engine_50x.run().await.unwrap();
    assert_eq!(stats_50x.total_records, 10);
    assert_eq!(stats_50x.partition_offsets, 10);

    // 3. Default 10x speed multiplier mode (speed_multiplier = 10.0)
    let sink_10x = Arc::new(StdoutSink::new_silent("orders/speed_10x"));
    let config_10x = ReplayConfig::builder()
        .file_path(path)
        .file_format("csv")
        .timestamp_column("order_purchase_timestamp")
        .key_column("order_id")
        .speed_multiplier(10.0)
        .progress(false)
        .build()
        .unwrap();

    let mut engine_10x = ReplayEngine::new(config_10x)
        .await
        .unwrap()
        .with_sink(sink_10x.clone());
    let stats_10x = engine_10x.run().await.unwrap();
    assert_eq!(stats_10x.total_records, 10);
    assert_eq!(stats_10x.partition_offsets, 10);
}
