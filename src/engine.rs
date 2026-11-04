use crate::config::ReplayConfig;
use crate::error::ReplayError;
use crate::reader::{create_reader_with_unit, detect_timestamp_column, RecordReader};
use crate::sink::{create_sink, Sink, SinkRecord, SinkType};
use crate::time_shift::{render_like, shift_value, TimeShiftEngine};
use chrono::Utc;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::sleep;
use tracing::{info, warn};

/// A process-unique id for this run: start time in nanoseconds plus the process id.
fn new_run_id() -> String {
    let nanos = Utc::now().timestamp_nanos_opt().unwrap_or_default();
    format!("{:x}-{:x}", nanos, std::process::id())
}

pub struct ReplayStats {
    pub total_records: u64,
    pub elapsed: std::time::Duration,
    pub loops_completed: u64,
    pub records_per_sec: f64,
    pub partition_offsets: u64,
}

pub struct ReplayEngine {
    config: ReplayConfig,
    timestamp_column: String,
    sink: Box<dyn Sink>,
    reader: Box<dyn RecordReader>,
    time_engine: TimeShiftEngine,
    custom_sink: Option<Arc<dyn Sink>>,
}

impl ReplayEngine {
    pub async fn new(config: ReplayConfig) -> Result<Self, ReplayError> {
        config.validate()?;

        let sink = create_sink(&config.sink).await?;
        let timestamp_column = match config.timestamp_column.clone() {
            Some(column) => column,
            None => {
                let detected = detect_timestamp_column(
                    &config.input_file_path,
                    config.input_file_format.as_deref(),
                )?;
                info!("Auto-detected timestamp column '{detected}'");
                detected
            }
        };
        let reader = create_reader_with_unit(
            &config.input_file_path,
            config.input_file_format.as_deref(),
            &timestamp_column,
            config.key_column.as_deref(),
            config.timestamp_unit,
        )?;

        // A fixed rate ignores the dataset spacing, so payload time follows the wall clock.
        let effective_speed = if config.rate.is_some() {
            0.0
        } else {
            config.speed_multiplier
        };
        let time_engine = TimeShiftEngine::new(
            config.shift_timestamps,
            config.advance_loop_timestamps,
            config.loop_id_salt,
        )
        .with_speed(effective_speed)
        .with_event_time(config.event_time);

        Ok(Self {
            config,
            timestamp_column,
            sink,
            reader,
            time_engine,
            custom_sink: None,
        })
    }

    pub fn with_sink(mut self, sink: Arc<dyn Sink>) -> Self {
        self.custom_sink = Some(sink);
        self
    }

    /// The pacing column in use (the configured one, or the auto-detected one).
    pub fn timestamp_column(&self) -> &str {
        &self.timestamp_column
    }

    fn limit_reached(&self, total_records: u64) -> bool {
        self.config
            .max_records
            .is_some_and(|limit| total_records >= limit)
    }

    pub async fn run(&mut self) -> Result<ReplayStats, ReplayError> {
        let mut total_records = 0u64;

        // Extract dataset bounds
        match self.reader.get_bounds() {
            Ok((t0, t_last)) => {
                self.time_engine.set_dataset_bounds(t0, t_last);
            }
            Err(e) => {
                warn!("Could not compute dataset bounds: {e}. Defaulting duration to 1 hour.");
                let now = Utc::now();
                self.time_engine
                    .set_dataset_bounds(now, now + chrono::Duration::hours(1));
            }
        }

        let pb = if self.config.progress {
            let progress = ProgressBar::new_spinner();
            progress.set_style(
                ProgressStyle::default_spinner()
                    .template("{spinner:.green} [{elapsed_precise}] {pos} records emitted ({per_sec}) | {msg}")
                    .unwrap_or_else(|_| ProgressStyle::default_spinner()),
            );
            progress.set_message("Replaying stream...");
            Some(progress)
        } else {
            None
        };

        let topic = match &self.config.sink {
            SinkType::Kafka { topic, .. } | SinkType::Stdout { topic } => topic.clone(),
        };
        // Identifies this process so that a duplicate caused by a retry can be recognised.
        let run_id = new_run_id();

        // Anchor the replay to the live clock: T_start = now(). The same moment starts the
        // wall-clock schedule, so event time and wall-clock time agree under `compress`.
        let replay_start = Instant::now();
        self.time_engine.start_time = Utc::now();
        let start_instant = replay_start;
        let mut warned_columns: HashSet<String> = HashSet::new();

        loop {
            if self.limit_reached(total_records) {
                break;
            }
            let mut records_in_pass = 0u64;

            while let Some(mut record) = self.reader.next_record()? {
                records_in_pass += 1;

                // Pacing: sleep until this record is due.
                let due = match self.config.rate {
                    Some(rate) => Some(std::time::Duration::from_secs_f64(
                        total_records as f64 / rate as f64,
                    )),
                    None => self
                        .time_engine
                        .schedule_offset(record.timestamp)
                        .and_then(|offset| offset.to_std().ok()),
                };
                if let Some(due) = due {
                    let deadline = replay_start + due;
                    let now = Instant::now();
                    if deadline > now {
                        sleep(deadline - now).await;
                    }
                }

                // Monotonic time transformation
                let raw_time = record.timestamp;
                let emitted_time = self.time_engine.calculate_emitted_time(raw_time);

                // ID salting
                let original_key = record.key.clone();
                if let Some(ref k) = record.key {
                    record.key = Some(self.time_engine.salt_entity_id(k));
                }

                if let serde_json::Value::Object(ref mut map) = record.payload {
                    if self.config.shift_timestamps {
                        let unit = self.config.timestamp_unit;
                        let offset = emitted_time - raw_time;

                        let rewritten = match map.get(&self.timestamp_column) {
                            Some(original) => render_like(original, emitted_time, unit),
                            None => serde_json::Value::String(emitted_time.to_rfc3339()),
                        };
                        map.insert(self.timestamp_column.clone(), rewritten);

                        for column in &self.config.shift_columns {
                            let Some(original) = map.get(column) else {
                                continue;
                            };
                            if original.is_null() {
                                continue;
                            }
                            match shift_value(original, offset, unit) {
                                Some(shifted) => {
                                    map.insert(column.clone(), shifted);
                                }
                                None => {
                                    if warned_columns.insert(column.clone()) {
                                        warn!(
                                            "Cannot interpret a value of shift column '{column}' as a timestamp; leaving it unchanged"
                                        );
                                    }
                                }
                            }
                        }
                    }

                    // Keep the key column in step with the salted entity key
                    if record.key != original_key {
                        if let (Some(k), Some(k_col)) = (
                            record.key.as_ref(),
                            self.config
                                .key_column
                                .as_ref()
                                .or(record.key_source_column.as_ref()),
                        ) {
                            map.insert(k_col.clone(), serde_json::Value::String(k.clone()));
                        }
                    }
                }

                let payload_bytes = serde_json::to_vec(&record.payload)?;

                // Dispatch to active sink. The headers give each record a stable identity
                // (run, ordinal, loop) so a retry duplicate can be found downstream.
                let sink_record = SinkRecord {
                    key: record.key.as_deref(),
                    payload: &payload_bytes,
                    timestamp: emitted_time,
                    headers: vec![
                        ("replay.run_id".to_string(), run_id.clone()),
                        ("replay.seq".to_string(), total_records.to_string()),
                        (
                            "replay.loop".to_string(),
                            self.time_engine.loop_count.to_string(),
                        ),
                    ],
                };
                if let Some(ref custom) = self.custom_sink {
                    custom.send_record(&topic, &sink_record).await?;
                } else {
                    self.sink.send_record(&topic, &sink_record).await?;
                }

                total_records += 1;
                if let Some(ref p) = pb {
                    p.inc(1);
                }

                if self.limit_reached(total_records) {
                    break;
                }
            }

            if self.limit_reached(total_records) {
                break;
            }

            if self.config.loop_playback && records_in_pass > 0 {
                self.time_engine.advance_loop();
                self.reader.reset()?;
                info!(
                    "Dataset EOF reached. Advanced to loop {}",
                    self.time_engine.loop_count
                );
            } else {
                break;
            }
        }

        if let Some(ref custom) = self.custom_sink {
            custom.flush().await?;
        } else {
            self.sink.flush().await?;
        }

        if let Some(ref p) = pb {
            p.finish_with_message(format!("Completed: {total_records} records emitted"));
        }

        let elapsed = start_instant.elapsed();
        let secs = elapsed.as_secs_f64();
        let records_per_sec = if secs > 0.000_001 {
            total_records as f64 / secs
        } else if total_records > 0 {
            total_records as f64 / 0.000_001
        } else {
            0.0
        };

        Ok(ReplayStats {
            total_records,
            elapsed,
            loops_completed: self.time_engine.loop_count,
            records_per_sec,
            partition_offsets: total_records,
        })
    }
}
