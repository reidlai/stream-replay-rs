use chrono::{Duration, TimeZone, Utc};
use stream_replay_rs::time_shift::{parse_timestamp, TimeShiftEngine};

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
