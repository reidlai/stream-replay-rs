use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use stream_replay_rs::reader::{create_reader, CsvRecordReader, ParquetRecordReader, RecordReader};
use tempfile::NamedTempFile;

#[test]
fn test_csv_reader_streaming() {
    let mut tmp = NamedTempFile::new().unwrap();
    writeln!(tmp, "order_id,timestamp,price").unwrap();
    writeln!(tmp, "ord_001,2026-10-30 10:00:00,49.99").unwrap();
    writeln!(tmp, "ord_002,2026-10-30 10:01:30,19.50").unwrap();
    writeln!(tmp, "ord_003,2026-10-30 10:05:00,120.00").unwrap();
    tmp.flush().unwrap();

    let mut reader = CsvRecordReader::new(tmp.path(), "timestamp", Some("order_id")).unwrap();

    let r1 = reader.next_record().unwrap().expect("record 1");
    assert_eq!(r1.key.as_deref(), Some("ord_001"));

    let r2 = reader.next_record().unwrap().expect("record 2");
    assert_eq!(r2.key.as_deref(), Some("ord_002"));

    let r3 = reader.next_record().unwrap().expect("record 3");
    assert_eq!(r3.key.as_deref(), Some("ord_003"));

    assert!(reader.next_record().unwrap().is_none());

    // Bounds check
    let (t0, t_last) = reader.get_bounds().unwrap();
    assert_eq!(r1.timestamp, t0);
    assert_eq!(r3.timestamp, t_last);
}

#[test]
fn test_csv_reader_missing_timestamp_column() {
    let mut tmp = NamedTempFile::new().unwrap();
    writeln!(tmp, "order_id,date,price").unwrap();
    writeln!(tmp, "ord_001,2026-10-30 10:00:00,49.99").unwrap();
    tmp.flush().unwrap();

    let res = CsvRecordReader::new(tmp.path(), "non_existent_timestamp", None);
    assert!(res.is_err());
}

#[test]
fn test_parquet_reader_streaming() {
    let tmp = NamedTempFile::new().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["pq_101", "pq_102", "pq_103"])),
            Arc::new(StringArray::from(vec![
                "2026-10-30T10:00:00Z",
                "2026-10-30T10:02:00Z",
                "2026-10-30T10:04:00Z",
            ])),
            Arc::new(Int64Array::from(vec![100, 250, 400])),
        ],
    )
    .unwrap();

    let file = File::create(tmp.path()).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let mut reader = ParquetRecordReader::new(tmp.path(), "timestamp", Some("order_id")).unwrap();

    let r1 = reader.next_record().unwrap().expect("record 1");
    assert_eq!(r1.key.as_deref(), Some("pq_101"));

    let r2 = reader.next_record().unwrap().expect("record 2");
    assert_eq!(r2.key.as_deref(), Some("pq_102"));

    let r3 = reader.next_record().unwrap().expect("record 3");
    assert_eq!(r3.key.as_deref(), Some("pq_103"));

    assert!(reader.next_record().unwrap().is_none());

    // Bounds check
    let (t0, t_last) = reader.get_bounds().unwrap();
    assert_eq!(r1.timestamp, t0);
    assert_eq!(r3.timestamp, t_last);

    // Auto-detect format via create_reader
    let mut dyn_reader = create_reader(tmp.path(), None, "timestamp", Some("order_id")).unwrap();
    let dyn_r1 = dyn_reader.next_record().unwrap().expect("dynamic record 1");
    assert_eq!(dyn_r1.key.as_deref(), Some("pq_101"));
}

#[test]
fn test_parquet_reader_native_timestamp_and_nulls() {
    use arrow::array::TimestampMicrosecondArray;
    use arrow::datatypes::TimeUnit;

    let tmp = NamedTempFile::new().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("amount", DataType::Int64, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["order_ts_1", "order_ts_2"])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                1_700_000_000_000_000,
                1_700_000_060_000_000,
            ])),
            Arc::new(Int64Array::from(vec![Some(50), None])),
        ],
    )
    .unwrap();

    let file = File::create(tmp.path()).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let mut reader = ParquetRecordReader::new(tmp.path(), "ts", Some("order_id")).unwrap();
    let r1 = reader.next_record().unwrap().expect("record 1");
    assert_eq!(r1.key.as_deref(), Some("order_ts_1"));
    assert_eq!(r1.payload["amount"], 50);

    let r2 = reader.next_record().unwrap().expect("record 2");
    assert_eq!(r2.key.as_deref(), Some("order_ts_2"));
    assert!(r2.payload["amount"].is_null());

    let (t0, t_last) = reader.get_bounds().unwrap();
    assert_eq!(r1.timestamp, t0);
    assert_eq!(r2.timestamp, t_last);
}
