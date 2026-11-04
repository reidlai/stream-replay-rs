//! Shared fixtures: tiny generated datasets with neutral column names.
#![allow(dead_code)]

use arrow::array::{Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, TimeZone, Utc};
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use tempfile::NamedTempFile;

/// Base time of the generated fixtures.
pub fn base_time() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2018, 1, 1, 0, 0, 0).unwrap()
}

/// Write `count` rows (`id`, `event_ts`, `value`) spaced `step_secs` apart to a Parquet file.
pub fn write_parquet_events(path: &Path, count: usize, step_secs: i64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new(
            "event_ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, false),
    ]));
    let base = base_time();
    let ids: Vec<String> = (0..count).map(|i| format!("evt_{i:04}")).collect();
    let stamps: Vec<i64> = (0..count)
        .map(|i| (base + chrono::Duration::seconds(i as i64 * step_secs)).timestamp_micros())
        .collect();
    let values: Vec<i64> = (0..count as i64).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(TimestampMicrosecondArray::from(stamps)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap();

    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// A CSV dataset `id,event_ts,ready_ts` whose `event_ts` values are given as offsets in
/// seconds from [`base_time`]; `ready_ts` is always `event_ts` plus 7 seconds.
pub fn csv_with_offsets(offsets_secs: &[i64]) -> NamedTempFile {
    let mut tmp = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    writeln!(tmp, "id,event_ts,ready_ts,value").unwrap();
    for (i, off) in offsets_secs.iter().enumerate() {
        let t = base_time() + chrono::Duration::seconds(*off);
        let ready = t + chrono::Duration::seconds(7);
        writeln!(
            tmp,
            "rec_{i},{},{},{}",
            t.to_rfc3339(),
            ready.to_rfc3339(),
            i * 10
        )
        .unwrap();
    }
    tmp.flush().unwrap();
    tmp
}
