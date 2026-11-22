use crate::error::ReplayError;
use crate::sink::SinkType;
use crate::time_shift::{EventTimeMode, TimestampUnit};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

pub const DEFAULT_SINK_URL: &str = "localhost:9092";
pub const DEFAULT_IGGY_URL: &str = "http://127.0.0.1:3000";
pub const DEFAULT_NATS_URL: &str = "nats://localhost:4222";
pub const DEFAULT_FLUVIO_URL: &str = "localhost:9003";
pub const DEFAULT_TOPIC: &str = "events";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SpeedSetting {
    Number(f64),
    Preset(String),
}

/// `[dataset]` table of a replay profile.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DatasetSection {
    pub input_path: Option<String>,
    pub format: Option<String>,
    pub timestamp_column: Option<String>,
    pub timestamp_unit: Option<TimestampUnit>,
    pub shift_columns: Option<Vec<String>>,
    pub partition_key: Option<String>,
}

/// `[replay]` table of a replay profile.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReplaySection {
    pub speed: Option<SpeedSetting>,
    pub rate: Option<u64>,
    pub event_time: Option<EventTimeMode>,
    pub shift_timestamps: Option<bool>,
    #[serde(rename = "loop", alias = "loop_playback")]
    pub loop_playback: Option<bool>,
    #[serde(alias = "advance_loop_timestamps")]
    pub advance_timestamps: Option<bool>,
    pub loop_id_salt: Option<bool>,
}

/// `[sink]` table of a replay profile.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SinkSection {
    pub default_type: Option<String>,
    pub default_url: Option<String>,
    pub destination: Option<String>,
    pub service_name: Option<String>,
}

/// `sink` is either a sink name (flat profile) or a `[sink]` table.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SinkSetting {
    Name(String),
    Table(SinkSection),
}

/// A replay profile. Settings may be written in `[dataset]`, `[replay]` and `[sink]`
/// tables, or as flat top-level keys; a table value wins over the flat key.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProfileConfig {
    pub dataset: Option<DatasetSection>,
    pub replay: Option<ReplaySection>,
    pub sink: Option<SinkSetting>,

    pub input_file_path: Option<String>,
    pub input_file_format: Option<String>,
    pub timestamp_column: Option<String>,
    pub timestamp_unit: Option<TimestampUnit>,
    pub shift_columns: Option<Vec<String>>,
    pub key_column: Option<String>,
    pub sink_url: Option<String>,
    pub topic: Option<String>,
    pub service_name: Option<String>,
    pub speed: Option<SpeedSetting>,
    pub rate: Option<u64>,
    pub event_time: Option<EventTimeMode>,
    pub shift_timestamps: Option<bool>,
    pub loop_playback: Option<bool>,
    pub advance_loop_timestamps: Option<bool>,
    pub loop_id_salt: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayConfig {
    pub input_file_path: PathBuf,
    pub input_file_format: Option<String>,
    pub sink: SinkType,
    /// Speed multiplier `s`: 1.0 keeps the original spacing, 10.0 is ten times faster,
    /// 0.0 is unthrottled. Ignored when `rate` is set.
    pub speed_multiplier: f64,
    /// Fixed events per second, ignoring the dataset's own spacing.
    pub rate: Option<u64>,
    pub shift_timestamps: bool,
    /// Pacing column; `None` auto-detects it.
    pub timestamp_column: Option<String>,
    pub timestamp_unit: TimestampUnit,
    /// Extra timestamp columns rebased by the same offset as the pacing column.
    pub shift_columns: Vec<String>,
    pub event_time: EventTimeMode,
    pub key_column: Option<String>,
    pub loop_playback: bool,
    pub advance_loop_timestamps: bool,
    pub loop_id_salt: bool,
    pub progress: bool,
    /// Stop after emitting this many records (library use; there is no CLI flag).
    pub max_records: Option<u64>,
}

pub fn parse_speed_preset(val: &str) -> Result<f64, ReplayError> {
    let s = val.trim().to_lowercase();
    match s.as_str() {
        "1x" | "1" => Ok(1.0),
        "10x" | "10" => Ok(10.0),
        "50x" | "50" => Ok(50.0),
        "max" | "unthrottled" | "0" | "0x" => Ok(0.0),
        other => {
            let clean = if let Some(stripped) = other.strip_suffix('x') {
                stripped.trim()
            } else {
                other
            };
            match clean.parse::<f64>() {
                Ok(num) if num.is_nan() || !num.is_finite() => {
                    Err(ReplayError::Config(format!(
                        "Invalid speed multiplier: '{val}' must be a finite real number or preset."
                    )))
                }
                Ok(num) => {
                    if num <= 0.0 {
                        Ok(0.0) // unthrottled
                    } else {
                        Ok(num)
                    }
                }
                Err(_) => Err(ReplayError::Config(format!(
                    "Invalid speed multiplier or preset: '{val}'. Expected a positive number such as 1, 2.5 or 10x, or 'max'."
                ))),
            }
        }
    }
}

/// Build a [`SinkType`] from its parts (unused parts are ignored by the chosen sink).
pub fn sink_from_parts(kind: &str, url: String, topic: String) -> Result<SinkType, ReplayError> {
    match kind.to_lowercase().as_str() {
        "kafka" => {
            #[cfg(feature = "kafka")]
            {
                Ok(SinkType::Kafka {
                    broker_url: url,
                    topic,
                })
            }
            #[cfg(not(feature = "kafka"))]
            {
                let _ = (url, topic);
                Err(ReplayError::Config(
                    "Kafka sink requested, but stream-replay-rs was compiled without '--features kafka'".into(),
                ))
            }
        }
        "iggy" => {
            #[cfg(feature = "iggy")]
            {
                let destination = topic.trim().to_string();
                if destination.is_empty() {
                    return Err(ReplayError::Config(
                        "Iggy destination must not be empty".into(),
                    ));
                }
                let trimmed_url = url.trim();
                let resolved_url = if trimmed_url.is_empty() || trimmed_url == DEFAULT_SINK_URL {
                    DEFAULT_IGGY_URL.to_string()
                } else {
                    trimmed_url.to_string()
                };
                Ok(SinkType::Iggy {
                    url: resolved_url,
                    destination,
                })
            }
            #[cfg(not(feature = "iggy"))]
            {
                let _ = (url, topic);
                Err(ReplayError::Config(
                    "Iggy sink requested, but stream-replay-rs was compiled without '--features iggy'".into(),
                ))
            }
        }
        "nats" => {
            #[cfg(feature = "nats")]
            {
                let subject = topic.trim().to_string();
                if subject.is_empty() {
                    return Err(ReplayError::Config("NATS subject must not be empty".into()));
                }
                let trimmed_url = url.trim();
                let resolved_url = if trimmed_url.is_empty() || trimmed_url == DEFAULT_SINK_URL {
                    DEFAULT_NATS_URL.to_string()
                } else {
                    trimmed_url.to_string()
                };
                Ok(SinkType::Nats {
                    url: resolved_url,
                    subject,
                })
            }
            #[cfg(not(feature = "nats"))]
            {
                let _ = (url, topic);
                Err(ReplayError::Config(
                    "NATS sink requested, but stream-replay-rs was compiled without '--features nats'".into(),
                ))
            }
        }
        "fluvio" => {
            #[cfg(feature = "fluvio")]
            {
                let topic = topic.trim().to_string();
                if topic.is_empty() {
                    return Err(ReplayError::Config("Fluvio topic must not be empty".into()));
                }
                let trimmed_url = url.trim();
                if !url.is_empty() && trimmed_url.is_empty() {
                    return Err(ReplayError::Config(
                        "Fluvio endpoint must not be empty".into(),
                    ));
                }
                let endpoint = if trimmed_url.is_empty() || trimmed_url == DEFAULT_SINK_URL {
                    DEFAULT_FLUVIO_URL.to_string()
                } else {
                    trimmed_url.to_string()
                };
                Ok(SinkType::Fluvio { endpoint, topic })
            }
            #[cfg(not(feature = "fluvio"))]
            {
                let _ = (url, topic);
                Err(ReplayError::Config(
                    "Fluvio sink requested, but stream-replay-rs was compiled without '--features fluvio'".into(),
                ))
            }
        }
        "iceoryx2" => {
            #[cfg(feature = "iceoryx2")]
            {
                let trimmed_topic = topic.trim();
                let trimmed_url = url.trim();
                let service_name = if !trimmed_topic.is_empty() && trimmed_topic != DEFAULT_TOPIC {
                    trimmed_topic.to_string()
                } else if (trimmed_topic.is_empty() || trimmed_topic == DEFAULT_TOPIC)
                    && !trimmed_url.is_empty()
                    && trimmed_url != DEFAULT_SINK_URL
                {
                    trimmed_url.to_string()
                } else {
                    trimmed_topic.to_string()
                };
                validate_iceoryx2_service_name(&service_name)?;
                Ok(SinkType::Iceoryx2 { service_name })
            }
            #[cfg(not(feature = "iceoryx2"))]
            {
                let _ = (url, topic);
                Err(ReplayError::Config(
                    "iceoryx2 sink requested, but stream-replay-rs was compiled without '--features iceoryx2'".into(),
                ))
            }
        }
        "stdout" => Ok(SinkType::Stdout { topic }),
        other => Err(ReplayError::Config(format!(
            "Unknown sink '{other}'. Supported sinks: kafka, iggy, nats, fluvio, iceoryx2, stdout."
        ))),
    }
}

/// Split a [`SinkType`] into `(kind, url, topic)`, filling defaults.
pub fn sink_parts(sink: &SinkType) -> (String, String, String) {
    match sink {
        #[cfg(feature = "kafka")]
        SinkType::Kafka { broker_url, topic } => {
            ("kafka".into(), broker_url.clone(), topic.clone())
        }
        #[cfg(feature = "iggy")]
        SinkType::Iggy { url, destination } => ("iggy".into(), url.clone(), destination.clone()),
        #[cfg(feature = "nats")]
        SinkType::Nats { url, subject } => ("nats".into(), url.clone(), subject.clone()),
        #[cfg(feature = "fluvio")]
        SinkType::Fluvio { endpoint, topic } => ("fluvio".into(), endpoint.clone(), topic.clone()),
        #[cfg(feature = "iceoryx2")]
        SinkType::Iceoryx2 { service_name } => ("iceoryx2".into(), "".into(), service_name.clone()),
        SinkType::Stdout { topic } => {
            ("stdout".into(), DEFAULT_SINK_URL.to_string(), topic.clone())
        }
    }
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            input_file_path: PathBuf::new(),
            input_file_format: None,
            sink: SinkType::Stdout {
                topic: DEFAULT_TOPIC.to_string(),
            },
            speed_multiplier: 1.0,
            rate: None,
            shift_timestamps: true,
            timestamp_column: None,
            timestamp_unit: TimestampUnit::Auto,
            shift_columns: Vec::new(),
            event_time: EventTimeMode::Compress,
            key_column: None,
            loop_playback: false,
            advance_loop_timestamps: false,
            loop_id_salt: false,
            progress: true,
            max_records: None,
        }
    }
}

pub struct ReplayConfigBuilder {
    config: ReplayConfig,
    speed_set: bool,
}

impl ReplayConfigBuilder {
    pub fn new() -> Self {
        Self {
            config: ReplayConfig::default(),
            speed_set: false,
        }
    }

    pub fn file_path(mut self, path: PathBuf) -> Self {
        self.config.input_file_path = path;
        self
    }

    pub fn file_format(mut self, format: impl Into<String>) -> Self {
        self.config.input_file_format = Some(format.into());
        self
    }

    pub fn sink(mut self, sink: SinkType) -> Self {
        #[cfg(feature = "fluvio")]
        let sink = match sink {
            SinkType::Fluvio { endpoint, topic } => SinkType::Fluvio {
                endpoint: endpoint.trim().to_string(),
                topic: topic.trim().to_string(),
            },
            other => other,
        };
        #[cfg(feature = "iceoryx2")]
        let sink = match sink {
            SinkType::Iceoryx2 { service_name } => SinkType::Iceoryx2 {
                service_name: service_name.trim().to_string(),
            },
            other => other,
        };
        self.config.sink = sink;
        self
    }

    pub fn stdout_sink(mut self, topic: impl Into<String>) -> Self {
        self.config.sink = SinkType::Stdout {
            topic: topic.into(),
        };
        self
    }

    #[cfg(feature = "kafka")]
    pub fn kafka_sink(mut self, broker_url: impl Into<String>, topic: impl Into<String>) -> Self {
        self.config.sink = SinkType::Kafka {
            broker_url: broker_url.into(),
            topic: topic.into(),
        };
        self
    }

    #[cfg(feature = "iggy")]
    pub fn iggy_sink(mut self, url: impl Into<String>, destination: impl Into<String>) -> Self {
        self.config.sink = SinkType::Iggy {
            url: url.into().trim().to_string(),
            destination: destination.into().trim().to_string(),
        };
        self
    }

    #[cfg(feature = "nats")]
    pub fn nats_sink(mut self, url: impl Into<String>, subject: impl Into<String>) -> Self {
        let url = url.into();
        let subject = subject.into();
        self.config.sink = SinkType::Nats {
            url: url.trim().to_string(),
            subject: subject.trim().to_string(),
        };
        self
    }

    #[cfg(feature = "fluvio")]
    pub fn fluvio_sink(mut self, endpoint: impl Into<String>, topic: impl Into<String>) -> Self {
        let endpoint = endpoint.into();
        let topic = topic.into();
        self.config.sink = SinkType::Fluvio {
            endpoint: endpoint.trim().to_string(),
            topic: topic.trim().to_string(),
        };
        self
    }

    #[cfg(feature = "iceoryx2")]
    pub fn iceoryx2_sink(mut self, service_name: impl Into<String>) -> Self {
        let service_name = service_name.into();
        self.config.sink = SinkType::Iceoryx2 {
            service_name: service_name.trim().to_string(),
        };
        self
    }

    pub fn speed_multiplier(mut self, speed: f64) -> Self {
        self.config.speed_multiplier = speed;
        self.speed_set = true;
        self
    }

    pub fn rate(mut self, rate: u64) -> Self {
        self.config.rate = Some(rate);
        self
    }

    pub fn shift_timestamps(mut self, shift: bool) -> Self {
        self.config.shift_timestamps = shift;
        self
    }

    pub fn timestamp_column(mut self, col: impl Into<String>) -> Self {
        self.config.timestamp_column = Some(col.into());
        self
    }

    pub fn timestamp_unit(mut self, unit: TimestampUnit) -> Self {
        self.config.timestamp_unit = unit;
        self
    }

    pub fn shift_column(mut self, col: impl Into<String>) -> Self {
        self.config.shift_columns.push(col.into());
        self
    }

    pub fn event_time(mut self, mode: EventTimeMode) -> Self {
        self.config.event_time = mode;
        self
    }

    pub fn key_column(mut self, col: impl Into<String>) -> Self {
        self.config.key_column = Some(col.into());
        self
    }

    pub fn loop_playback(mut self, loop_pb: bool) -> Self {
        self.config.loop_playback = loop_pb;
        self
    }

    pub fn advance_loop_timestamps(mut self, advance: bool) -> Self {
        self.config.advance_loop_timestamps = advance;
        self
    }

    pub fn loop_id_salt(mut self, salt: bool) -> Self {
        self.config.loop_id_salt = salt;
        self
    }

    pub fn progress(mut self, progress: bool) -> Self {
        self.config.progress = progress;
        self
    }

    pub fn max_records(mut self, max_records: u64) -> Self {
        self.config.max_records = Some(max_records);
        self
    }

    pub fn build(self) -> Result<ReplayConfig, ReplayError> {
        if self.speed_set && self.config.rate.is_some() {
            return Err(ReplayError::Config(
                "speed and rate are mutually exclusive".to_string(),
            ));
        }
        self.config.validate()?;
        Ok(self.config)
    }
}

impl Default for ReplayConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "iceoryx2")]
fn validate_iceoryx2_service_name(service_name: &str) -> Result<(), ReplayError> {
    let trimmed = service_name.trim();
    if trimmed.is_empty() {
        return Err(ReplayError::Config(
            "iceoryx2 service_name must not be empty".to_string(),
        ));
    }
    iceoryx2::prelude::ServiceName::new(trimmed).map_err(|e| {
        ReplayError::Config(format!("Invalid iceoryx2 service_name '{trimmed}': {e}"))
    })?;
    Ok(())
}

impl ReplayConfig {
    pub fn builder() -> ReplayConfigBuilder {
        ReplayConfigBuilder::new()
    }

    /// Check the settings that do not depend on the dataset contents.
    pub fn validate(&self) -> Result<(), ReplayError> {
        if self.input_file_path.as_os_str().is_empty() {
            return Err(ReplayError::Config(
                "Input file path must not be empty".to_string(),
            ));
        }
        if self.rate == Some(0) {
            return Err(ReplayError::Config(
                "rate must be at least 1 event per second".to_string(),
            ));
        }
        if !self.speed_multiplier.is_finite() || self.speed_multiplier < 0.0 {
            return Err(ReplayError::Config(format!(
                "Invalid speed multiplier: {}",
                self.speed_multiplier
            )));
        }
        #[cfg(feature = "nats")]
        if let SinkType::Nats { url, subject } = &self.sink {
            if url.trim().is_empty() {
                return Err(ReplayError::Config(
                    "NATS endpoint URL must not be empty".to_string(),
                ));
            }
            if subject.trim().is_empty() {
                return Err(ReplayError::Config(
                    "NATS subject must not be empty".to_string(),
                ));
            }
        }
        #[cfg(feature = "fluvio")]
        if let SinkType::Fluvio { endpoint, topic } = &self.sink {
            if endpoint.trim().is_empty() {
                return Err(ReplayError::Config(
                    "Fluvio endpoint must not be empty".to_string(),
                ));
            }
            if topic.trim().is_empty() {
                return Err(ReplayError::Config(
                    "Fluvio topic must not be empty".to_string(),
                ));
            }
        }
        #[cfg(feature = "iceoryx2")]
        if let SinkType::Iceoryx2 { service_name } = &self.sink {
            validate_iceoryx2_service_name(service_name)?;
        }
        Ok(())
    }

    pub fn from_profile_file(path: &Path) -> Result<Self, ReplayError> {
        let content = fs::read_to_string(path)?;
        let profile: ProfileConfig = toml::from_str(&content)
            .map_err(|e| ReplayError::Config(format!("Failed to parse profile TOML: {e}")))?;
        Self::from_profile(profile)
    }

    /// Resolve a parsed profile into a config. The input path may be left empty so
    /// that a command-line flag can supply it; call [`ReplayConfig::validate`] after.
    pub fn from_profile(profile: ProfileConfig) -> Result<Self, ReplayError> {
        let dataset = profile.dataset.unwrap_or_default();
        let replay = profile.replay.unwrap_or_default();
        let (sink_name, sink_table) = match profile.sink {
            Some(SinkSetting::Name(name)) => (Some(name), SinkSection::default()),
            Some(SinkSetting::Table(table)) => (None, table),
            None => (None, SinkSection::default()),
        };

        let sink_kind = sink_table
            .default_type
            .or(sink_name)
            .unwrap_or_else(|| "stdout".to_string());
        let has_explicit_dest = sink_table.service_name.is_some()
            || sink_table.destination.is_some()
            || profile.service_name.is_some()
            || profile.topic.is_some();
        let resolved_dest = sink_table
            .service_name
            .filter(|s| !s.trim().is_empty())
            .or_else(|| sink_table.destination.filter(|s| !s.trim().is_empty()))
            .or_else(|| profile.service_name.filter(|s| !s.trim().is_empty()))
            .or_else(|| profile.topic.filter(|s| !s.trim().is_empty()))
            .unwrap_or_else(|| {
                if has_explicit_dest {
                    String::new()
                } else {
                    DEFAULT_TOPIC.to_string()
                }
            });
        let resolved_url = match sink_table.default_url.or(profile.sink_url) {
            Some(u) if u.trim().is_empty() => "   ".to_string(),
            Some(u) => u,
            None => DEFAULT_SINK_URL.to_string(),
        };
        let sink = sink_from_parts(&sink_kind, resolved_url, resolved_dest)?;

        let speed_setting = replay.speed.or(profile.speed);
        let rate = replay.rate.or(profile.rate);
        if speed_setting.is_some() && rate.is_some() {
            return Err(ReplayError::Config(
                "speed and rate are mutually exclusive; set only one in the profile".to_string(),
            ));
        }
        let speed_multiplier = match speed_setting {
            Some(SpeedSetting::Number(num)) => {
                if !num.is_finite() {
                    return Err(ReplayError::Config(format!(
                        "Invalid speed multiplier: {num}"
                    )));
                }
                num.max(0.0)
            }
            Some(SpeedSetting::Preset(preset_str)) => parse_speed_preset(&preset_str)?,
            None => 1.0,
        };

        Ok(Self {
            input_file_path: PathBuf::from(
                dataset
                    .input_path
                    .or(profile.input_file_path)
                    .unwrap_or_default(),
            ),
            input_file_format: dataset.format.or(profile.input_file_format),
            sink,
            speed_multiplier,
            rate,
            shift_timestamps: replay
                .shift_timestamps
                .or(profile.shift_timestamps)
                .unwrap_or(true),
            timestamp_column: dataset.timestamp_column.or(profile.timestamp_column),
            timestamp_unit: dataset
                .timestamp_unit
                .or(profile.timestamp_unit)
                .unwrap_or_default(),
            shift_columns: dataset
                .shift_columns
                .or(profile.shift_columns)
                .unwrap_or_default(),
            event_time: replay.event_time.or(profile.event_time).unwrap_or_default(),
            key_column: dataset.partition_key.or(profile.key_column),
            loop_playback: replay
                .loop_playback
                .or(profile.loop_playback)
                .unwrap_or(false),
            advance_loop_timestamps: replay
                .advance_timestamps
                .or(profile.advance_loop_timestamps)
                .unwrap_or(false),
            loop_id_salt: replay
                .loop_id_salt
                .or(profile.loop_id_salt)
                .unwrap_or(false),
            progress: true,
            max_records: None,
        })
    }
}
