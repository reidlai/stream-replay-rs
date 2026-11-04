mod common;

use std::io::Write;
use std::sync::Arc;
use stream_replay_rs::config::{parse_speed_preset, ReplayConfig};
use stream_replay_rs::engine::ReplayEngine;
use stream_replay_rs::error::ReplayError;
use stream_replay_rs::sink::stdout::StdoutSink;
use tempfile::NamedTempFile;

#[test]
fn test_default_speed_is_one() {
    let default_cfg = ReplayConfig::default();
    assert_eq!(default_cfg.speed_multiplier, 1.0);
    assert_eq!(default_cfg.rate, None);
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
    assert_eq!(parse_speed_preset("0.5").unwrap(), 0.5);
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
input_file_path = "data/events.parquet"
speed = "50x"
"#
    )
    .unwrap();

    let cfg = ReplayConfig::from_profile_file(tmp.path()).unwrap();
    assert_eq!(cfg.speed_multiplier, 50.0);

    // Profile omitting speed must default to 1.0 (original spacing)
    let mut tmp_default = NamedTempFile::new().unwrap();
    write!(
        tmp_default,
        r#"
input_file_path = "data/events.parquet"
"#
    )
    .unwrap();

    let cfg_default = ReplayConfig::from_profile_file(tmp_default.path()).unwrap();
    assert_eq!(cfg_default.speed_multiplier, 1.0);

    // Numeric float in TOML
    let mut tmp_numeric = NamedTempFile::new().unwrap();
    write!(
        tmp_numeric,
        r#"
input_file_path = "data/events.parquet"
speed = 25.5
"#
    )
    .unwrap();

    let cfg_numeric = ReplayConfig::from_profile_file(tmp_numeric.path()).unwrap();
    assert_eq!(cfg_numeric.speed_multiplier, 25.5);
}

#[test]
fn test_speed_and_rate_conflict_in_builder_and_profile() {
    let err = ReplayConfig::builder()
        .file_path("events.csv".into())
        .speed_multiplier(10.0)
        .rate(25)
        .build()
        .unwrap_err();
    assert!(matches!(err, ReplayError::Config(ref m) if m.contains("mutually exclusive")));

    // Either one alone is fine.
    assert!(ReplayConfig::builder()
        .file_path("events.csv".into())
        .rate(25)
        .build()
        .is_ok());
    assert!(ReplayConfig::builder()
        .file_path("events.csv".into())
        .speed_multiplier(10.0)
        .build()
        .is_ok());

    let mut profile = NamedTempFile::new().unwrap();
    write!(
        profile,
        r#"
input_file_path = "events.csv"
speed = 10.0
rate = 25
"#
    )
    .unwrap();
    let err = ReplayConfig::from_profile_file(profile.path()).unwrap_err();
    assert!(matches!(err, ReplayError::Config(ref m) if m.contains("mutually exclusive")));
}

#[tokio::test]
async fn test_unthrottled_parquet_fixture_replay() {
    let tmp = tempfile::Builder::new()
        .suffix(".parquet")
        .tempfile()
        .unwrap();
    common::write_parquet_events(tmp.path(), 500, 60);

    let sink = Arc::new(StdoutSink::new_silent("events/unthrottled"));
    let config = ReplayConfig::builder()
        .file_path(tmp.path().to_path_buf())
        .timestamp_column("event_ts")
        .key_column("id")
        .speed_multiplier(0.0) // max: no pacing
        .progress(false)
        .build()
        .unwrap();

    let mut engine = ReplayEngine::new(config)
        .await
        .unwrap()
        .with_sink(sink.clone());
    let stats = engine.run().await.unwrap();

    assert_eq!(stats.total_records, 500);
    assert_eq!(sink.get_recorded().len(), 500);
    assert!(
        stats.records_per_sec >= 500.0,
        "Unthrottled throughput was {:.1} msgs/s (expected >= 500 msgs/s)",
        stats.records_per_sec
    );
}

#[tokio::test]
async fn test_max_records_zero_boundary() {
    let tmp = tempfile::Builder::new()
        .suffix(".parquet")
        .tempfile()
        .unwrap();
    common::write_parquet_events(tmp.path(), 10, 1);

    let sink = Arc::new(StdoutSink::new_silent("events/zero"));
    let config = ReplayConfig::builder()
        .file_path(tmp.path().to_path_buf())
        .timestamp_column("event_ts")
        .key_column("id")
        .max_records(0)
        .progress(false)
        .build()
        .unwrap();

    let mut engine = ReplayEngine::new(config)
        .await
        .unwrap()
        .with_sink(sink.clone());
    let stats = engine.run().await.unwrap();

    assert_eq!(stats.total_records, 0);
    assert_eq!(sink.get_recorded().len(), 0);
}

#[tokio::test]
async fn test_engine_replay_across_speed_multipliers() {
    let mut tmp = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    writeln!(tmp, "id,event_ts,data").unwrap();
    for i in 0..10 {
        writeln!(tmp, "ord_{i},2026-01-01T00:00:00.{:03}Z,payload_{i}", i * 5).unwrap();
    }
    let path = tmp.path().to_path_buf();

    for speed in [0.0, 1.0, 10.0, 50.0] {
        let sink = Arc::new(StdoutSink::new_silent("events/speed"));
        let config = ReplayConfig::builder()
            .file_path(path.clone())
            .timestamp_column("event_ts")
            .key_column("id")
            .speed_multiplier(speed)
            .progress(false)
            .build()
            .unwrap();

        let mut engine = ReplayEngine::new(config)
            .await
            .unwrap()
            .with_sink(sink.clone());
        let stats = engine.run().await.unwrap();
        assert_eq!(stats.total_records, 10, "speed {speed}");
        assert_eq!(sink.get_recorded().len(), 10, "speed {speed}");
    }
}

#[tokio::test]
async fn test_speed_one_keeps_original_spacing_on_the_wall_clock() {
    // Records 1 s apart: at speed 1 the replay takes about 1 s, at speed 20 about 50 ms.
    let tmp = common::csv_with_offsets(&[0, 1]);
    let path = tmp.path().to_path_buf();

    let run = |speed: f64| {
        let path = path.clone();
        async move {
            let sink = Arc::new(StdoutSink::new_silent("events/spacing"));
            let config = ReplayConfig::builder()
                .file_path(path)
                .timestamp_column("event_ts")
                .speed_multiplier(speed)
                .progress(false)
                .build()
                .unwrap();
            let mut engine = ReplayEngine::new(config)
                .await
                .unwrap()
                .with_sink(sink.clone());
            engine.run().await.unwrap().elapsed
        }
    };

    let slow = run(1.0).await;
    assert!(
        slow >= std::time::Duration::from_millis(950),
        "speed 1 should keep the 1 s gap, took {slow:?}"
    );
    let fast = run(20.0).await;
    assert!(
        fast < std::time::Duration::from_millis(500),
        "speed 20 should shrink the gap to 50 ms, took {fast:?}"
    );
}
