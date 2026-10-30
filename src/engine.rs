use crate::auth::M2MAuthenticator;
use crate::config::ReplayConfig;
use crate::error::ReplayError;
use crate::reader::{create_reader, RecordReader};
use crate::sink::{create_sink, Sink, SinkType};
use crate::time_shift::TimeShiftEngine;
use chrono::Utc;
use indicatif::{ProgressBar, ProgressStyle};
use std::sync::Arc;
use std::time::Instant;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

pub struct ReplayStats {
    pub total_records: u64,
    pub elapsed: std::time::Duration,
    pub loops_completed: u64,
    pub records_per_sec: f64,
    pub partition_offsets: u64,
}

pub struct ReplayEngine {
    config: ReplayConfig,
    sink: Box<dyn Sink>,
    reader: Box<dyn RecordReader>,
    time_engine: TimeShiftEngine,
    authenticator: M2MAuthenticator,
    custom_sink: Option<Arc<dyn Sink>>,
}

impl ReplayEngine {
    pub async fn new(config: ReplayConfig) -> Result<Self, ReplayError> {
        let sink = create_sink(&config.sink).await?;
        let reader = create_reader(
            &config.input_file_path,
            config.input_file_format.as_deref(),
            &config.timestamp_column,
            config.key_column.as_deref(),
        )?;

        let time_engine = TimeShiftEngine::new(
            config.shift_timestamps,
            config.advance_loop_timestamps,
            config.loop_id_salt,
        );

        let authenticator = M2MAuthenticator::new(
            config.auth_url.clone(),
            config.service_account.clone(),
            config.service_secret.clone(),
            config.auth_token.clone(),
        );

        Ok(Self {
            config,
            sink,
            reader,
            time_engine,
            authenticator,
            custom_sink: None,
        })
    }

    pub fn with_sink(mut self, sink: Arc<dyn Sink>) -> Self {
        self.custom_sink = Some(sink);
        self
    }

    pub fn authenticator(&self) -> &M2MAuthenticator {
        &self.authenticator
    }

    pub async fn run(&mut self) -> Result<ReplayStats, ReplayError> {
        let mut total_records = 0u64;

        // Perform M2M authentication pre-flight
        let _token = self.authenticator.authenticate().await?;
        info!(
            "M2M Authentication active for service account '{}'",
            self.authenticator.service_account
        );

        // Extract dataset bounds
        match self.reader.get_bounds() {
            Ok((t0, t_last)) => {
                self.time_engine.set_dataset_bounds(t0, t_last);
            }
            Err(e) => {
                warn!("Could not compute dataset bounds: {e}. Defaulting duration to 1 hour.");
                let now = Utc::now();
                self.time_engine.set_dataset_bounds(now, now + chrono::Duration::hours(1));
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
            SinkType::Kafka { topic, .. }
            | SinkType::Nats { topic, .. }
            | SinkType::Fluvio { topic, .. }
            | SinkType::Stdout { topic } => topic.clone(),
            SinkType::Iceoryx2 { service_name } => service_name.clone(),
        };

        let mut first_emitted: Option<chrono::DateTime<Utc>> = None;
        let mut first_instant: Option<Instant> = None;

        let max_records = self.config.burst;
        let start_instant = Instant::now();

        loop {
            if let Some(burst_limit) = max_records {
                if total_records as usize >= burst_limit {
                    break;
                }
            }

            while let Some(mut record) = self.reader.next_record()? {
                // Monotonic time transformation
                let emitted_time = self.time_engine.calculate_emitted_time(record.timestamp);

                // ID salting
                if let Some(ref k) = record.key {
                    record.key = Some(self.time_engine.salt_entity_id(k));
                }

                // Update payload timestamp field
                if let serde_json::Value::Object(ref mut map) = record.payload {
                    map.insert(
                        self.config.timestamp_column.clone(),
                        serde_json::Value::String(emitted_time.to_rfc3339()),
                    );
                    if let Some(ref k) = record.key {
                        if let Some(k_col) = self.config.key_column.as_ref().or(record.key_source_column.as_ref()) {
                            map.insert(k_col.clone(), serde_json::Value::String(k.clone()));
                        }
                    }
                }

                // Rate / pacing control
                if max_records.is_none() {
                    if let Some(rate) = self.config.rate {
                        if let Some(interval_micros) = 1_000_000u64.checked_div(rate) {
                            sleep(Duration::from_micros(interval_micros)).await;
                        }
                    } else if self.config.speed_multiplier > 0.0 {
                        if let (Some(t_first), Some(i_first)) = (first_emitted, first_instant) {
                            let dataset_elapsed = (emitted_time - t_first).num_microseconds().unwrap_or(0);
                            if dataset_elapsed > 0 {
                                let scaled_micros = (dataset_elapsed as f64 / self.config.speed_multiplier) as u64;
                                let actual_elapsed_micros = i_first.elapsed().as_micros() as u64;
                                if scaled_micros > actual_elapsed_micros {
                                    let diff = scaled_micros - actual_elapsed_micros;
                                    // Cap sleep to prevent excessive hangs on sparse datasets
                                    sleep(Duration::from_micros(diff.min(10_000_000))).await;
                                }
                            }
                        } else {
                            first_emitted = Some(emitted_time);
                            first_instant = Some(Instant::now());
                        }
                    }
                }

                let payload_bytes = serde_json::to_vec(&record.payload)?;

                // Dispatch to active sink
                if let Some(ref custom) = self.custom_sink {
                    custom
                        .send(&topic, record.key.as_deref(), &payload_bytes)
                        .await?;
                } else {
                    self.sink
                        .send(&topic, record.key.as_deref(), &payload_bytes)
                        .await?;
                }

                total_records += 1;
                if let Some(ref p) = pb {
                    p.inc(1);
                }

                if let Some(burst_limit) = max_records {
                    if total_records as usize >= burst_limit {
                        break;
                    }
                }
            }

            if let Some(burst_limit) = max_records {
                if total_records as usize >= burst_limit {
                    break;
                }
            }

            if self.config.loop_playback {
                self.time_engine.advance_loop();
                self.reader.reset()?;
                first_emitted = None;
                first_instant = None;
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

