use chrono::{Duration, TimeZone, Utc};
use serde_json::json;
use stream_replay_rs::time_shift::{
    detect_epoch_unit, parse_timestamp, parse_timestamp_with_unit, shift_value, EventTimeMode,
    TimeShiftEngine, TimestampUnit,
};

#[test]
fn test_parse_timestamp_formats() {
    // RFC 3339
    let dt1 = parse_timestamp("2026-10-30T10:00:00Z").unwrap();
    assert_eq!(dt1, Utc.with_ymd_and_hms(2026, 10, 30, 10, 0, 0).unwrap());

    // Standard database format
    let dt2 = parse_timestamp("2017-10-02 10:56:33").unwrap();
    assert_eq!(dt2, Utc.with_ymd_and_hms(2017, 10, 2, 10, 56, 33).unwrap());

    // Unix epoch seconds
    let dt3 = parse_timestamp("1506941793").unwrap();
    assert_eq!(dt3.timestamp(), 1506941793);

    // Unix epoch milliseconds
    let dt4 = parse_timestamp("1506941793000").unwrap();
    assert_eq!(dt4.timestamp(), 1506941793);

    // Malformed string
    assert!(parse_timestamp("not-a-timestamp").is_err());
}

#[test]
fn test_monotonic_time_shifting_single_loop() {
    let mut engine = TimeShiftEngine::new(true, false, false);
    let t0 = Utc.with_ymd_and_hms(2017, 1, 1, 0, 0, 0).unwrap();
    let t1 = t0 + Duration::seconds(10);
    let t2 = t0 + Duration::seconds(25);
    let t3 = t0 + Duration::seconds(60);

    engine.set_dataset_bounds(t0, t3);

    let emitted0 = engine.calculate_emitted_time(t0);
    let emitted1 = engine.calculate_emitted_time(t1);
    let emitted2 = engine.calculate_emitted_time(t2);
    let emitted3 = engine.calculate_emitted_time(t3);

    // Relative intervals should be exactly preserved
    assert_eq!((emitted1 - emitted0).num_seconds(), 10);
    assert_eq!((emitted2 - emitted1).num_seconds(), 15);
    assert_eq!((emitted3 - emitted2).num_seconds(), 35);

    // Strictly monotonic
    assert!(emitted1 > emitted0);
    assert!(emitted2 > emitted1);
    assert!(emitted3 > emitted2);
}

#[test]
fn test_multi_loop_monotonic_time_advance() {
    let mut engine = TimeShiftEngine::new(true, true, true);
    let t0 = Utc.with_ymd_and_hms(2017, 1, 1, 0, 0, 0).unwrap();
    let t1 = t0 + Duration::seconds(100);
    let dataset_duration = Duration::seconds(100);

    engine.set_dataset_bounds(t0, t1);

    // Loop 0
    let emitted_l0_start = engine.calculate_emitted_time(t0);
    let emitted_l0_end = engine.calculate_emitted_time(t1);
    assert_eq!((emitted_l0_end - emitted_l0_start).num_seconds(), 100);

    // Advance to Loop 1
    engine.advance_loop();
    assert_eq!(engine.loop_count, 1);

    let emitted_l1_start = engine.calculate_emitted_time(t0);
    let emitted_l1_end = engine.calculate_emitted_time(t1);

    // Loop 1 start must be after Loop 0 end
    assert!(emitted_l1_start >= emitted_l0_end);
    assert_eq!(
        (emitted_l1_start - emitted_l0_start).num_seconds(),
        dataset_duration.num_seconds()
    );
    assert!(emitted_l1_end > emitted_l1_start);

    // Advance to Loop 2
    engine.advance_loop();
    assert_eq!(engine.loop_count, 2);

    let emitted_l2_start = engine.calculate_emitted_time(t0);
    assert!(emitted_l2_start > emitted_l1_end);
}

#[test]
fn test_loop_id_salting() {
    let mut engine = TimeShiftEngine::new(true, true, true);
    let order_id = "order_7a12b";

    // Loop 0: no salt
    assert_eq!(engine.salt_entity_id(order_id), "order_7a12b");

    // Loop 1: _L1 salt
    engine.advance_loop();
    assert_eq!(engine.salt_entity_id(order_id), "order_7a12b_L1");

    // Loop 2: _L2 salt
    engine.advance_loop();
    assert_eq!(engine.salt_entity_id(order_id), "order_7a12b_L2");

    // When salting is disabled
    let engine_no_salt = TimeShiftEngine::new(true, true, false);
    assert_eq!(engine_no_salt.salt_entity_id(order_id), "order_7a12b");
}

#[test]
fn test_monotonicity_with_out_of_order_raw_data() {
    let mut engine = TimeShiftEngine::new(true, false, false);
    let t0 = Utc.with_ymd_and_hms(2017, 1, 1, 0, 0, 10).unwrap();
    let t_earlier = Utc.with_ymd_and_hms(2017, 1, 1, 0, 0, 5).unwrap(); // backwards!

    let emitted0 = engine.calculate_emitted_time(t0);
    let emitted1 = engine.calculate_emitted_time(t_earlier);

    // Must still advance monotonically by at least 1ns
    assert!(emitted1 > emitted0);
}

fn example_engine(speed: f64, mode: EventTimeMode) -> (TimeShiftEngine, chrono::DateTime<Utc>) {
    // The three-record dataset of the README worked example: 0 s, 30 s and 120 s.
    let t0 = Utc.with_ymd_and_hms(2018, 1, 1, 0, 0, 0).unwrap();
    let mut engine = TimeShiftEngine::new(true, true, false)
        .with_speed(speed)
        .with_event_time(mode);
    engine.set_dataset_bounds(t0, t0 + Duration::seconds(120));
    (engine, t0)
}

#[test]
fn test_compress_divides_offsets_by_speed() {
    let (mut engine, t0) = example_engine(10.0, EventTimeMode::Compress);
    let start = engine.start_time;
    assert_eq!(engine.calculate_emitted_time(t0), start);
    assert_eq!(
        engine.calculate_emitted_time(t0 + Duration::seconds(30)),
        start + Duration::seconds(3)
    );
    assert_eq!(
        engine.calculate_emitted_time(t0 + Duration::seconds(120)),
        start + Duration::seconds(12)
    );

    // Second pass: Delta_dataset / speed is added, as in the README example
    engine.advance_loop();
    assert_eq!(
        engine.calculate_emitted_time(t0 + Duration::seconds(30)),
        start + Duration::seconds(15)
    );
}

#[test]
fn test_preserve_keeps_original_spacing_at_any_speed() {
    let (mut engine, t0) = example_engine(10.0, EventTimeMode::Preserve);
    let start = engine.start_time;
    assert_eq!(engine.calculate_emitted_time(t0), start);
    assert_eq!(
        engine.calculate_emitted_time(t0 + Duration::seconds(30)),
        start + Duration::seconds(30)
    );
    assert_eq!(
        engine.calculate_emitted_time(t0 + Duration::seconds(120)),
        start + Duration::seconds(120)
    );
    // Pacing still follows the speed
    assert_eq!(
        engine.schedule_offset(t0 + Duration::seconds(120)),
        Some(Duration::seconds(12))
    );
}

#[test]
fn test_compress_and_preserve_agree_at_speed_one() {
    for mode in [EventTimeMode::Compress, EventTimeMode::Preserve] {
        let (mut engine, t0) = example_engine(1.0, mode);
        let start = engine.start_time;
        assert_eq!(
            engine.calculate_emitted_time(t0 + Duration::seconds(30)),
            start + Duration::seconds(30)
        );
    }
}

#[test]
fn test_slow_speed_stretches_offsets() {
    let (mut engine, t0) = example_engine(0.5, EventTimeMode::Compress);
    let start = engine.start_time;
    assert_eq!(
        engine.calculate_emitted_time(t0 + Duration::seconds(30)),
        start + Duration::seconds(60)
    );
}

#[test]
fn test_schedule_offset_includes_loops_and_is_none_when_unthrottled() {
    let (mut engine, t0) = example_engine(10.0, EventTimeMode::Compress);
    engine.advance_loop();
    engine.advance_loop();
    // (30 s + 2 * 120 s) / 10
    assert_eq!(
        engine.schedule_offset(t0 + Duration::seconds(30)),
        Some(Duration::seconds(27))
    );

    let (mut unthrottled, t0) = example_engine(0.0, EventTimeMode::Compress);
    assert_eq!(unthrottled.schedule_offset(t0), None);
}

#[test]
fn test_monotonic_across_loops_in_both_modes() {
    for mode in [EventTimeMode::Compress, EventTimeMode::Preserve] {
        let (mut engine, t0) = example_engine(10.0, mode);
        let mut previous = None;
        for _ in 0..4 {
            for offset in [0, 30, 120] {
                let emitted = engine.calculate_emitted_time(t0 + Duration::seconds(offset));
                if let Some(prev) = previous {
                    assert!(emitted > prev, "{mode:?}: {emitted} !> {prev}");
                }
                previous = Some(emitted);
            }
            engine.advance_loop();
        }
    }
}

#[test]
fn test_unthrottled_compress_follows_the_wall_clock() {
    let (mut engine, t0) = example_engine(0.0, EventTimeMode::Compress);
    let before = Utc::now();
    let emitted = engine.calculate_emitted_time(t0 + Duration::seconds(120));
    assert!(emitted >= before && emitted <= Utc::now() + Duration::seconds(1));
}

#[test]
fn test_shifting_disabled_returns_raw_time() {
    let mut engine = TimeShiftEngine::new(false, true, false).with_speed(10.0);
    let raw = Utc.with_ymd_and_hms(2018, 1, 1, 0, 0, 30).unwrap();
    assert_eq!(engine.calculate_emitted_time(raw), raw);
}

#[test]
fn test_explicit_timestamp_units() {
    // 500_000_000_000 is seconds to auto-detect but milliseconds when told so
    let auto = parse_timestamp("500000000000").unwrap();
    assert_eq!(auto.timestamp(), 500_000_000_000);
    let ms = parse_timestamp_with_unit("500000000000", TimestampUnit::Millis).unwrap();
    assert_eq!(ms, Utc.with_ymd_and_hms(1985, 11, 5, 0, 53, 20).unwrap());

    let s = parse_timestamp_with_unit("1506941793", TimestampUnit::Seconds).unwrap();
    assert_eq!(s.timestamp(), 1506941793);
    let us = parse_timestamp_with_unit("1506941793000000", TimestampUnit::Micros).unwrap();
    assert_eq!(us.timestamp(), 1506941793);
    let ns = parse_timestamp_with_unit("1506941793000000000", TimestampUnit::Nanos).unwrap();
    assert_eq!(ns.timestamp(), 1506941793);

    // rfc3339 accepts text only, a numeric unit also accepts text
    assert!(parse_timestamp_with_unit("1506941793", TimestampUnit::Rfc3339).is_err());
    assert!(parse_timestamp_with_unit("2026-10-30T10:00:00Z", TimestampUnit::Rfc3339).is_ok());
    assert!(parse_timestamp_with_unit("2026-10-30T10:00:00Z", TimestampUnit::Millis).is_ok());
    // Naive text is UTC
    assert_eq!(
        parse_timestamp_with_unit("2017-10-02 10:56:33", TimestampUnit::Rfc3339).unwrap(),
        Utc.with_ymd_and_hms(2017, 10, 2, 10, 56, 33).unwrap()
    );

    assert_eq!(detect_epoch_unit(1_506_941_793), TimestampUnit::Seconds);
    assert_eq!(detect_epoch_unit(1_506_941_793_000), TimestampUnit::Millis);
    assert_eq!(
        detect_epoch_unit(1_506_941_793_000_000),
        TimestampUnit::Micros
    );
    assert_eq!(
        detect_epoch_unit(1_506_941_793_000_000_000),
        TimestampUnit::Nanos
    );
}

#[test]
fn test_shift_value_keeps_representation() {
    let offset = Duration::seconds(90);

    let text = shift_value(&json!("2018-01-01T00:00:00Z"), offset, TimestampUnit::Auto).unwrap();
    assert_eq!(text, json!("2018-01-01T00:01:30+00:00"));

    let millis = shift_value(&json!(1_514_764_800_000_i64), offset, TimestampUnit::Auto).unwrap();
    assert_eq!(millis, json!(1_514_764_890_000_i64));

    let seconds = shift_value(&json!(1_514_764_800), offset, TimestampUnit::Seconds).unwrap();
    assert_eq!(seconds, json!(1_514_764_890));

    assert!(shift_value(&json!("not a time"), offset, TimestampUnit::Auto).is_none());
    assert!(shift_value(&json!(true), offset, TimestampUnit::Auto).is_none());
}
