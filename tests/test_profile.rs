use std::io::Write;
use std::path::Path;
use stream_replay_rs::config::ReplayConfig;
use stream_replay_rs::error::ReplayError;
use stream_replay_rs::sink::SinkType;
use stream_replay_rs::time_shift::{EventTimeMode, TimestampUnit};
use tempfile::NamedTempFile;

fn example_profile() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("config/profiles/example.toml")
}

#[test]
fn test_example_profile_loads_with_documented_keys() {
    let cfg = ReplayConfig::from_profile_file(&example_profile()).unwrap();
    assert_eq!(cfg.input_file_path, Path::new("./data/events.parquet"));
    assert_eq!(cfg.input_file_format.as_deref(), Some("parquet"));
    assert_eq!(cfg.timestamp_column.as_deref(), Some("event_time"));
    assert_eq!(cfg.timestamp_unit, TimestampUnit::Auto);
    assert_eq!(cfg.shift_columns, vec!["processed_time"]);
    assert_eq!(cfg.key_column.as_deref(), Some("event_id"));
    assert_eq!(cfg.speed_multiplier, 1.0);
    assert_eq!(cfg.rate, None);
    assert_eq!(cfg.event_time, EventTimeMode::Compress);
    assert!(cfg.loop_playback);
    assert!(cfg.advance_loop_timestamps);
    assert!(cfg.loop_id_salt);
    assert_eq!(
        cfg.sink,
        SinkType::Stdout {
            topic: "events".to_string()
        }
    );
    cfg.validate().unwrap();
}

#[test]
fn test_sectioned_profile_with_kafka_sink_and_fixed_rate() {
    let mut tmp = NamedTempFile::new().unwrap();
    write!(
        tmp,
        r#"
[dataset]
input_path = "events.csv"
timestamp_unit = "ms"

[replay]
rate = 25
event_time = "preserve"

[sink]
default_type = "kafka"
default_url = "broker:9092"
destination = "replayed"
"#
    )
    .unwrap();

    let cfg = ReplayConfig::from_profile_file(tmp.path()).unwrap();
    assert_eq!(cfg.rate, Some(25));
    assert_eq!(cfg.timestamp_unit, TimestampUnit::Millis);
    assert_eq!(cfg.event_time, EventTimeMode::Preserve);
    assert_eq!(
        cfg.sink,
        SinkType::Kafka {
            broker_url: "broker:9092".to_string(),
            topic: "replayed".to_string()
        }
    );
}

#[test]
fn test_sectioned_speed_and_rate_conflict() {
    let mut tmp = NamedTempFile::new().unwrap();
    write!(
        tmp,
        r#"
[dataset]
input_path = "events.csv"

[replay]
speed = 10
rate = 25
"#
    )
    .unwrap();
    let err = ReplayConfig::from_profile_file(tmp.path()).unwrap_err();
    assert!(matches!(err, ReplayError::Config(_)));
}

#[test]
fn test_legacy_flat_profile_keys_and_ignored_unknown_keys() {
    let mut tmp = NamedTempFile::new().unwrap();
    write!(
        tmp,
        r#"
input_file_path = "events.csv"
sink = "kafka"
sink_url = "broker:9092"
topic = "subject"
speed = "10x"
loop_playback = true
"#
    )
    .unwrap();
    let cfg = ReplayConfig::from_profile_file(tmp.path()).unwrap();
    assert_eq!(cfg.speed_multiplier, 10.0);
    assert!(cfg.loop_playback);
    assert_eq!(
        cfg.sink,
        SinkType::Kafka {
            broker_url: "broker:9092".to_string(),
            topic: "subject".to_string()
        }
    );
}

#[test]
fn test_removed_sinks_are_rejected() {
    for removed in ["nats", "fluvio", "iceoryx2"] {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(
            tmp,
            "input_file_path = \"events.csv\"\nsink = \"{removed}\"\n"
        )
        .unwrap();
        assert!(
            ReplayConfig::from_profile_file(tmp.path()).is_err(),
            "{removed} must not be accepted"
        );
    }
}

#[test]
fn test_unknown_sink_is_rejected() {
    let mut tmp = NamedTempFile::new().unwrap();
    write!(
        tmp,
        r#"
input_file_path = "events.csv"
sink = "carrier-pigeon"
"#
    )
    .unwrap();
    assert!(ReplayConfig::from_profile_file(tmp.path()).is_err());
}
