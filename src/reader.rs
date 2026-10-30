use crate::error::ReplayError;
use crate::time_shift::parse_timestamp;
use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::DataType;
use chrono::{DateTime, TimeZone, Utc};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Map, Value};
use std::fs::File;
use std::path::{Path, PathBuf};
use tracing::warn;

#[derive(Debug, Clone)]
pub struct ReplayRecord {
    pub key: Option<String>,
    pub key_source_column: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub payload: Value,
}

pub trait RecordReader: Send {
    fn next_record(&mut self) -> Result<Option<ReplayRecord>, ReplayError>;
    fn reset(&mut self) -> Result<(), ReplayError>;
    fn get_bounds(&mut self) -> Result<(DateTime<Utc>, DateTime<Utc>), ReplayError>;
}

pub struct CsvRecordReader {
    path: PathBuf,
    timestamp_col: String,
    key_col: Option<String>,
    csv_reader: csv::Reader<File>,
    headers: Vec<String>,
}

impl CsvRecordReader {
    pub fn new(
        path: &Path,
        timestamp_col: &str,
        key_col: Option<&str>,
    ) -> Result<Self, ReplayError> {
        let file = File::open(path)?;
        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(file);

        let headers = csv_reader
            .headers()?
            .iter()
            .map(|h| h.trim().to_string())
            .collect::<Vec<_>>();

        if !headers.iter().any(|h| h == timestamp_col) {
            return Err(ReplayError::Dataset(format!(
                "Timestamp column '{}' not found in CSV headers: {:?}",
                timestamp_col, headers
            )));
        }

        Ok(Self {
            path: path.to_path_buf(),
            timestamp_col: timestamp_col.to_string(),
            key_col: key_col.map(|s| s.to_string()),
            csv_reader,
            headers,
        })
    }
}

impl RecordReader for CsvRecordReader {
    fn next_record(&mut self) -> Result<Option<ReplayRecord>, ReplayError> {
        let mut record = csv::StringRecord::new();
        loop {
            match self.csv_reader.read_record(&mut record) {
                Ok(false) => return Ok(None),
                Ok(true) => break,
                Err(e) => {
                    warn!("Skipping malformed CSV row: {e}");
                    continue;
                }
            }
        }

        let mut map = Map::new();
        let mut raw_timestamp: Option<DateTime<Utc>> = None;
        let mut key: Option<String> = None;
        let mut key_source_column: Option<String> = None;

        for (i, field) in record.iter().enumerate() {
            if let Some(col_name) = self.headers.get(i) {
                let val_str = field.trim();
                let json_val = if let Ok(num) = val_str.parse::<i64>() {
                    Value::Number(num.into())
                } else if let Ok(flt) = val_str.parse::<f64>() {
                    serde_json::Number::from_f64(flt)
                        .map_or(Value::String(val_str.to_string()), Value::Number)
                } else if let Ok(b) = val_str.parse::<bool>() {
                    Value::Bool(b)
                } else {
                    Value::String(val_str.to_string())
                };

                if col_name == &self.timestamp_col {
                    match parse_timestamp(val_str) {
                        Ok(dt) => raw_timestamp = Some(dt),
                        Err(e) => {
                            warn!("Failed to parse timestamp in row: {}. Using now().", e);
                            raw_timestamp = Some(Utc::now());
                        }
                    }
                }

                if let Some(ref k_col) = self.key_col {
                    if col_name == k_col {
                        key = Some(val_str.to_string());
                        key_source_column = Some(col_name.clone());
                    }
                } else if key.is_none() && (col_name.contains("id") || col_name.contains("key")) {
                    key = Some(val_str.to_string());
                    key_source_column = Some(col_name.clone());
                }

                map.insert(col_name.clone(), json_val);
            }
        }

        let timestamp = raw_timestamp.unwrap_or_else(Utc::now);

        Ok(Some(ReplayRecord {
            key,
            key_source_column,
            timestamp,
            payload: Value::Object(map),
        }))
    }

    fn reset(&mut self) -> Result<(), ReplayError> {
        let file = File::open(&self.path)?;
        self.csv_reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(file);
        Ok(())
    }

    fn get_bounds(&mut self) -> Result<(DateTime<Utc>, DateTime<Utc>), ReplayError> {
        self.reset()?;
        let first = self.next_record()?.ok_or_else(|| {
            ReplayError::Dataset("Cannot compute bounds on empty CSV dataset".to_string())
        })?;
        let mut t_min = first.timestamp;
        let mut t_max = first.timestamp;

        while let Some(rec) = self.next_record()? {
            t_min = t_min.min(rec.timestamp);
            t_max = t_max.max(rec.timestamp);
        }

        self.reset()?;
        Ok((t_min, t_max))
    }
}

pub struct ParquetRecordReader {
    path: PathBuf,
    timestamp_col: String,
    key_col: Option<String>,
    batch_reader: Box<dyn arrow::record_batch::RecordBatchReader + Send>,
    current_batch: Option<RecordBatch>,
    current_row: usize,
}

impl ParquetRecordReader {
    pub fn new(
        path: &Path,
        timestamp_col: &str,
        key_col: Option<&str>,
    ) -> Result<Self, ReplayError> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema();

        if schema.field_with_name(timestamp_col).is_err() {
            return Err(ReplayError::Dataset(format!(
                "Timestamp column '{}' not found in Parquet schema: {:?}",
                timestamp_col,
                schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
            )));
        }

        let batch_reader = Box::new(builder.with_batch_size(2048).build()?);

        Ok(Self {
            path: path.to_path_buf(),
            timestamp_col: timestamp_col.to_string(),
            key_col: key_col.map(|s| s.to_string()),
            batch_reader,
            current_batch: None,
            current_row: 0,
        })
    }

    fn extract_record_from_batch(
        batch: &RecordBatch,
        row: usize,
        timestamp_col: &str,
        key_col: &Option<String>,
    ) -> Result<ReplayRecord, ReplayError> {
        let mut map = Map::new();
        let schema = batch.schema();
        let mut raw_timestamp: Option<DateTime<Utc>> = None;
        let mut key: Option<String> = None;
        let mut key_source_column: Option<String> = None;

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col_name = field.name();
            let col = batch.column(col_idx);

            if col.is_null(row) {
                map.insert(col_name.clone(), Value::Null);
                continue;
            }

            let (json_val, str_rep): (Value, Option<String>) = match col.data_type() {
                DataType::Utf8 => {
                    let arr = col.as_string::<i32>();
                    let s = arr.value(row);
                    (Value::String(s.to_string()), Some(s.to_string()))
                }
                DataType::LargeUtf8 => {
                    let arr = col.as_string::<i64>();
                    let s = arr.value(row);
                    (Value::String(s.to_string()), Some(s.to_string()))
                }
                DataType::Int64 => {
                    let arr = col.as_primitive::<arrow::datatypes::Int64Type>();
                    let v = arr.value(row);
                    (Value::Number(v.into()), Some(v.to_string()))
                }
                DataType::Int32 => {
                    let arr = col.as_primitive::<arrow::datatypes::Int32Type>();
                    let v = arr.value(row);
                    (Value::Number(v.into()), Some(v.to_string()))
                }
                DataType::Int16 => {
                    let arr = col.as_primitive::<arrow::datatypes::Int16Type>();
                    let v = arr.value(row);
                    (Value::Number(v.into()), Some(v.to_string()))
                }
                DataType::Int8 => {
                    let arr = col.as_primitive::<arrow::datatypes::Int8Type>();
                    let v = arr.value(row);
                    (Value::Number(v.into()), Some(v.to_string()))
                }
                DataType::UInt64 => {
                    let arr = col.as_primitive::<arrow::datatypes::UInt64Type>();
                    let v = arr.value(row);
                    (Value::Number(v.into()), Some(v.to_string()))
                }
                DataType::UInt32 => {
                    let arr = col.as_primitive::<arrow::datatypes::UInt32Type>();
                    let v = arr.value(row);
                    (Value::Number(v.into()), Some(v.to_string()))
                }
                DataType::Float64 => {
                    let arr = col.as_primitive::<arrow::datatypes::Float64Type>();
                    let v = arr.value(row);
                    let num = serde_json::Number::from_f64(v).map_or(Value::Null, Value::Number);
                    (num, Some(v.to_string()))
                }
                DataType::Float32 => {
                    let arr = col.as_primitive::<arrow::datatypes::Float32Type>();
                    let v = arr.value(row);
                    let num =
                        serde_json::Number::from_f64(v as f64).map_or(Value::Null, Value::Number);
                    (num, Some(v.to_string()))
                }
                DataType::Timestamp(unit, _) => {
                    let dt = match unit {
                        arrow::datatypes::TimeUnit::Second => {
                            let arr = col.as_primitive::<arrow::datatypes::TimestampSecondType>();
                            Utc.timestamp_opt(arr.value(row), 0).single()
                        }
                        arrow::datatypes::TimeUnit::Millisecond => {
                            let arr =
                                col.as_primitive::<arrow::datatypes::TimestampMillisecondType>();
                            Utc.timestamp_millis_opt(arr.value(row)).single()
                        }
                        arrow::datatypes::TimeUnit::Microsecond => {
                            let arr =
                                col.as_primitive::<arrow::datatypes::TimestampMicrosecondType>();
                            Utc.timestamp_micros(arr.value(row)).single()
                        }
                        arrow::datatypes::TimeUnit::Nanosecond => {
                            let arr =
                                col.as_primitive::<arrow::datatypes::TimestampNanosecondType>();
                            let v = arr.value(row);
                            Utc.timestamp_opt(v / 1_000_000_000, (v % 1_000_000_000) as u32)
                                .single()
                        }
                    };
                    let dt_val = dt.unwrap_or_else(Utc::now);
                    if col_name == timestamp_col {
                        raw_timestamp = Some(dt_val);
                    }
                    (
                        Value::String(dt_val.to_rfc3339()),
                        Some(dt_val.to_rfc3339()),
                    )
                }
                DataType::Boolean => {
                    let arr = col.as_boolean();
                    let v = arr.value(row);
                    (Value::Bool(v), Some(v.to_string()))
                }
                _ => (Value::String(format!("{:?}", col)), None),
            };

            if col_name == timestamp_col && raw_timestamp.is_none() {
                if let Some(ref s) = str_rep {
                    match parse_timestamp(s) {
                        Ok(dt) => raw_timestamp = Some(dt),
                        Err(_) => raw_timestamp = Some(Utc::now()),
                    }
                }
            }

            if let Some(ref k) = key_col {
                if col_name == k {
                    if let Value::String(ref s) = json_val {
                        key = Some(s.clone());
                        key_source_column = Some(col_name.clone());
                    } else if let Some(ref s) = str_rep {
                        key = Some(s.clone());
                        key_source_column = Some(col_name.clone());
                    }
                }
            } else if key.is_none() && (col_name.contains("id") || col_name.contains("key")) {
                if let Value::String(ref s) = json_val {
                    key = Some(s.clone());
                    key_source_column = Some(col_name.clone());
                } else if let Some(ref s) = str_rep {
                    key = Some(s.clone());
                    key_source_column = Some(col_name.clone());
                }
            }

            map.insert(col_name.clone(), json_val);
        }

        let timestamp = raw_timestamp.unwrap_or_else(Utc::now);

        Ok(ReplayRecord {
            key,
            key_source_column,
            timestamp,
            payload: Value::Object(map),
        })
    }
}

impl RecordReader for ParquetRecordReader {
    fn next_record(&mut self) -> Result<Option<ReplayRecord>, ReplayError> {
        loop {
            if let Some(ref batch) = self.current_batch {
                if self.current_row < batch.num_rows() {
                    let rec = Self::extract_record_from_batch(
                        batch,
                        self.current_row,
                        &self.timestamp_col,
                        &self.key_col,
                    )?;
                    self.current_row += 1;
                    return Ok(Some(rec));
                }
            }

            // Need next batch
            match self.batch_reader.next() {
                Some(Ok(batch)) => {
                    self.current_batch = Some(batch);
                    self.current_row = 0;
                }
                Some(Err(e)) => return Err(ReplayError::Arrow(e)),
                None => {
                    self.current_batch = None;
                    return Ok(None);
                }
            }
        }
    }

    fn reset(&mut self) -> Result<(), ReplayError> {
        let file = File::open(&self.path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        self.batch_reader = Box::new(builder.with_batch_size(2048).build()?);
        self.current_batch = None;
        self.current_row = 0;
        Ok(())
    }

    fn get_bounds(&mut self) -> Result<(DateTime<Utc>, DateTime<Utc>), ReplayError> {
        self.reset()?;
        let first = self.next_record()?.ok_or_else(|| {
            ReplayError::Dataset("Cannot compute bounds on empty Parquet dataset".to_string())
        })?;
        let mut t_min = first.timestamp;
        let mut t_max = first.timestamp;

        while let Some(rec) = self.next_record()? {
            t_min = t_min.min(rec.timestamp);
            t_max = t_max.max(rec.timestamp);
        }

        self.reset()?;
        Ok((t_min, t_max))
    }
}

pub fn create_reader(
    path: &Path,
    format_opt: Option<&str>,
    timestamp_col: &str,
    key_col: Option<&str>,
) -> Result<Box<dyn RecordReader>, ReplayError> {
    let format = match format_opt {
        Some(f) => f.to_lowercase(),
        None => {
            if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
                ext.to_lowercase()
            } else {
                // Inspect first 4 bytes for Parquet magic header PAR1
                let mut header = [0u8; 4];
                if let Ok(mut f) = File::open(path) {
                    use std::io::Read;
                    if f.read_exact(&mut header).is_ok() && &header == b"PAR1" {
                        "parquet".to_string()
                    } else {
                        "csv".to_string()
                    }
                } else {
                    "csv".to_string()
                }
            }
        }
    };

    match format.as_str() {
        "parquet" | "pq" => Ok(Box::new(ParquetRecordReader::new(
            path,
            timestamp_col,
            key_col,
        )?)),
        "csv" => Ok(Box::new(CsvRecordReader::new(
            path,
            timestamp_col,
            key_col,
        )?)),
        other => Err(ReplayError::Config(format!(
            "Unsupported dataset format: '{}'. Expected 'csv' or 'parquet'",
            other
        ))),
    }
}
