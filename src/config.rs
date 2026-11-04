use crate::error::ReplayError;
use crate::sink::SinkType;
use crate::time_shift::{EventTimeMode, TimestampUnit};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

pub const DEFAULT_SINK_URL: &str = "localhost:9092";
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
        "kafka" => Ok(SinkType::Kafka {
            broker_url: url,
            topic,
        }),
        "stdout" => Ok(SinkType::Stdout { topic }),
        other => Err(ReplayError::Config(format!(
            "Unknown sink '{other}'. Expected kafka or stdout."
        ))),
    }
}

/// Split a [`SinkType`] into `(kind, url, topic)`, filling defaults.
pub fn sink_parts(sink: &SinkType) -> (String, String, String) {
    match sink {
        SinkType::Kafka { broker_url, topic } => {
            ("kafka".into(), broker_url.clone(), topic.clone())
        }
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
        self.config.sink = sink;
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
        let sink = sink_from_parts(
            &sink_kind,
            sink_table
                .default_url
                .or(profile.sink_url)
                .unwrap_or_else(|| DEFAULT_SINK_URL.to_string()),
            sink_table
                .destination
                .or(profile.topic)
                .unwrap_or_else(|| DEFAULT_TOPIC.to_string()),
        )?;

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
