mod common;

use chrono::{DateTime, Duration, Utc};
use std::io::Write;
use std::sync::Arc;
use stream_replay_rs::config::{ReplayConfig, ReplayConfigBuilder};
use stream_replay_rs::engine::{ReplayEngine, ReplayStats};
use stream_replay_rs::sink::stdout::StdoutSink;
use stream_replay_rs::time_shift::{EventTimeMode, TimestampUnit};

/// Offsets of the README worked example: 0 s, 30 s and 120 s from the first record.
const EXAMPLE_OFFSETS: [i64; 3] = [0, 30, 120];

async fn run(builder: ReplayConfigBuilder) -> (ReplayStats, Vec<serde_json::Value>) {
    let sink = Arc::new(StdoutSink::new_silent("events/test"));
    let config: ReplayConfig = builder.progress(false).build().unwrap();
    let mut engine = ReplayEngine::new(config)
        .await
        .unwrap()
        .with_sink(sink.clone());
    let stats = engine.run().await.unwrap();
    let payloads = sink
        .get_recorded()
        .iter()
        .map(|(_, _, bytes)| serde_json::from_slice(bytes).unwrap())
        .collect();
    (stats, payloads)
}

fn ts(payload: &serde_json::Value, column: &str) -> DateTime<Utc> {
    payload[column].as_str().unwrap().parse().unwrap()
}

fn example_builder(path: &std::path::Path) -> ReplayConfigBuilder {
    ReplayConfig::builder()
        .file_path(path.to_path_buf())
        .timestamp_column("event_ts")
        .key_column("id")
}

#[tokio::test]
async fn test_compress_divides_payload_timestamps_by_speed() {
    let csv = common::csv_with_offsets(&EXAMPLE_OFFSETS);
    let before = Utc::now();
    let (stats, rows) = run(example_builder(csv.path()).speed_multiplier(1000.0)).await;
    assert_eq!(stats.total_records, 3);

    let t: Vec<_> = rows.iter().map(|r| ts(r, "event_ts")).collect();
    // 30 s and 120 s divided by 1000
    assert_eq!(t[1] - t[0], Duration::milliseconds(30));
    assert_eq!(t[2] - t[0], Duration::milliseconds(120));
    // Anchored on the live clock, not on the 2018 file times
    assert!(t[0] >= before - Duration::seconds(1));
    assert!(t[0] <= Utc::now() + Duration::seconds(1));
}

#[tokio::test]
async fn test_default_event_time_mode_is_compress() {
    let csv = common::csv_with_offsets(&EXAMPLE_OFFSETS);
    let (_, explicit) = run(example_builder(csv.path())
        .speed_multiplier(1000.0)
        .event_time(EventTimeMode::Compress))
    .await;
    let (_, default) = run(example_builder(csv.path()).speed_multiplier(1000.0)).await;
    assert_eq!(
        ts(&explicit[2], "event_ts") - ts(&explicit[0], "event_ts"),
        ts(&default[2], "event_ts") - ts(&default[0], "event_ts"),
    );
}

#[tokio::test]
async fn test_preserve_keeps_original_payload_spacing_while_pacing_follows_speed() {
    let csv = common::csv_with_offsets(&EXAMPLE_OFFSETS);
    let (stats, rows) = run(example_builder(csv.path())
        .speed_multiplier(1000.0)
        .event_time(EventTimeMode::Preserve))
    .await;

    let t: Vec<_> = rows.iter().map(|r| ts(r, "event_ts")).collect();
    assert_eq!(t[1] - t[0], Duration::seconds(30));
    assert_eq!(t[2] - t[0], Duration::seconds(120));
    // Pacing still followed the speed: 120 s of data took far less than 2 s.
    assert!(stats.elapsed < std::time::Duration::from_secs(2));
}

#[tokio::test]
async fn test_speed_one_compress_and_preserve_agree() {
    let csv = common::csv_with_offsets(&[0, 1]);
    for mode in [EventTimeMode::Compress, EventTimeMode::Preserve] {
        let (_, rows) = run(example_builder(csv.path()).event_time(mode)).await;
        let t: Vec<_> = rows.iter().map(|r| ts(r, "event_ts")).collect();
        assert_eq!(t[1] - t[0], Duration::seconds(1), "{mode:?}");
    }
}

#[tokio::test]
async fn test_shift_column_moves_by_the_same_offset() {
    let csv = common::csv_with_offsets(&EXAMPLE_OFFSETS);
    for mode in [EventTimeMode::Compress, EventTimeMode::Preserve] {
        let (_, rows) = run(example_builder(csv.path())
            .speed_multiplier(1000.0)
            .event_time(mode)
            .shift_column("ready_ts"))
        .await;
        for row in &rows {
            // ready_ts was always 7 s after event_ts and must stay 7 s after it.
            assert_eq!(
                ts(row, "ready_ts") - ts(row, "event_ts"),
                Duration::seconds(7),
                "{mode:?}"
            );
        }
        assert!(ts(&rows[0], "ready_ts") > Utc::now() - Duration::minutes(5));
    }

    // Without --shift-column the other timestamp column is left as in the file.
    let (_, rows) = run(example_builder(csv.path()).speed_multiplier(1000.0)).await;
    assert_eq!(
        ts(&rows[0], "ready_ts").to_rfc3339(),
        "2018-01-01T00:00:07+00:00"
    );
}

#[tokio::test]
async fn test_no_shift_timestamps_passes_payload_through_but_still_paces() {
    let csv = common::csv_with_offsets(&[0, 1]);
    let (stats, rows) = run(example_builder(csv.path())
        .speed_multiplier(10.0)
        .shift_timestamps(false)
        .shift_column("ready_ts"))
    .await;
    assert_eq!(
        ts(&rows[0], "event_ts").to_rfc3339(),
        "2018-01-01T00:00:00+00:00"
    );
    assert_eq!(
        ts(&rows[1], "event_ts").to_rfc3339(),
        "2018-01-01T00:00:01+00:00"
    );
    assert_eq!(
        ts(&rows[1], "ready_ts").to_rfc3339(),
        "2018-01-01T00:00:08+00:00"
    );
    // 1 s of data at 10x takes about 100 ms.
    assert!(stats.elapsed >= std::time::Duration::from_millis(90));
}

#[tokio::test]
async fn test_timestamp_unit_milliseconds_round_trips_as_epoch_numbers() {
    // 500_000_000_000 is below the auto-detect threshold, so auto would read it as
    // seconds. With --timestamp-unit ms it is 1985, and the payload stays in milliseconds.
    let mut tmp = tempfile::Builder::new()
        .suffix(".jsonl")
        .tempfile()
        .unwrap();
    writeln!(tmp, r#"{{"id":"a","ts":500000000000,"done":500000005000}}"#).unwrap();
    writeln!(tmp, r#"{{"id":"b","ts":500000030000,"done":500000035000}}"#).unwrap();
    tmp.flush().unwrap();

    let before_ms = Utc::now().timestamp_millis();
    let (_, rows) = run(ReplayConfig::builder()
        .file_path(tmp.path().to_path_buf())
        .timestamp_column("ts")
        .timestamp_unit(TimestampUnit::Millis)
        .shift_column("done")
        .key_column("id")
        .speed_multiplier(1000.0))
    .await;

    let t0 = rows[0]["ts"].as_i64().unwrap();
    let t1 = rows[1]["ts"].as_i64().unwrap();
    assert!(
        t0 >= before_ms - 1000,
        "{t0} should be a current epoch in ms"
    );
    assert_eq!(t1 - t0, 30); // 30 000 ms / 1000
    assert_eq!(rows[0]["done"].as_i64().unwrap() - t0, 5000);
    assert_eq!(rows[1]["done"].as_i64().unwrap() - t1, 5000);
}

#[tokio::test]
async fn test_emitted_timestamps_are_monotonic_across_loops_with_advance() {
    let csv = common::csv_with_offsets(&EXAMPLE_OFFSETS);
    let (stats, rows) = run(example_builder(csv.path())
        .speed_multiplier(1000.0)
        .loop_playback(true)
        .advance_loop_timestamps(true)
        .max_records(9)) // 3 records * 3 loops
    .await;
    assert_eq!(stats.total_records, 9);
    assert_eq!(stats.loops_completed, 2);

    let t: Vec<_> = rows.iter().map(|r| ts(r, "event_ts")).collect();
    for pair in t.windows(2) {
        assert!(pair[1] > pair[0], "{} !> {}", pair[1], pair[0]);
    }
    // Loop 1 starts one dataset span (120 s / 1000) after loop 0 started.
    let span = Duration::milliseconds(120);
    assert!(t[3] - t[0] >= span);
    assert!(t[6] - t[3] >= span);
    assert!(t[3] - t[0] <= span + Duration::microseconds(1));
}

#[tokio::test]
async fn test_loops_without_advance_repeat_timestamps_but_pacing_continues() {
    let csv = common::csv_with_offsets(&EXAMPLE_OFFSETS);
    let (stats, rows) = run(example_builder(csv.path())
        .speed_multiplier(1000.0)
        .loop_playback(true)
        .max_records(6))
    .await;
    assert_eq!(stats.total_records, 6);

    let t: Vec<_> = rows.iter().map(|r| ts(r, "event_ts")).collect();
    assert_eq!(t[3], t[0]);
    // The second pass is still paced after the first: its last record is due 240 ms in.
    assert!(stats.elapsed >= std::time::Duration::from_millis(200));
}

#[tokio::test]
async fn test_loop_with_id_salt_and_advance_in_preserve_mode_is_monotonic() {
    let csv = common::csv_with_offsets(&EXAMPLE_OFFSETS);
    let (_, rows) = run(example_builder(csv.path())
        .speed_multiplier(1000.0)
        .event_time(EventTimeMode::Preserve)
        .loop_playback(true)
        .advance_loop_timestamps(true)
        .loop_id_salt(true)
        .max_records(6))
    .await;

    let t: Vec<_> = rows.iter().map(|r| ts(r, "event_ts")).collect();
    for pair in t.windows(2) {
        assert!(pair[1] > pair[0]);
    }
    // One dataset span later (plus the 1 ns monotonic nudge at the loop boundary)
    assert!(t[3] - t[0] >= Duration::seconds(120));
    assert!(t[3] - t[0] <= Duration::seconds(120) + Duration::microseconds(1));
    assert_eq!(rows[0]["id"], "rec_0");
    assert_eq!(rows[3]["id"], "rec_0_L1");
}

#[tokio::test]
async fn test_fixed_rate_paces_independently_of_dataset_spacing() {
    let csv = common::csv_with_offsets(&[0, 3600, 7200, 10800, 14400]);
    let (stats, rows) = run(example_builder(csv.path()).rate(50)).await;
    assert_eq!(stats.total_records, 5);
    // Fifth record is due 4 / 50 = 80 ms in; an hour of dataset spacing is ignored.
    assert!(stats.elapsed >= std::time::Duration::from_millis(70));
    assert!(stats.elapsed < std::time::Duration::from_secs(2));
    // Payload time follows the wall clock under compress.
    let span = ts(&rows[4], "event_ts") - ts(&rows[0], "event_ts");
    assert!(span >= Duration::milliseconds(70), "{span}");
    assert!(span < Duration::seconds(2), "{span}");
}

#[tokio::test]
async fn test_timestamp_column_is_auto_detected() {
    let csv = common::csv_with_offsets(&EXAMPLE_OFFSETS);
    let config = ReplayConfig::builder()
        .file_path(csv.path().to_path_buf())
        .speed_multiplier(1000.0)
        .progress(false)
        .build()
        .unwrap();
    let engine = ReplayEngine::new(config).await.unwrap();
    assert_eq!(engine.timestamp_column(), "event_ts");

    let pq = tempfile::Builder::new()
        .suffix(".parquet")
        .tempfile()
        .unwrap();
    common::write_parquet_events(pq.path(), 3, 10);
    let config = ReplayConfig::builder()
        .file_path(pq.path().to_path_buf())
        .progress(false)
        .build()
        .unwrap();
    let engine = ReplayEngine::new(config).await.unwrap();
    assert_eq!(engine.timestamp_column(), "event_ts");
}
