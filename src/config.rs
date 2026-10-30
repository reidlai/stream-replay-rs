use crate::error::ReplayError;
use crate::sink::SinkType;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileConfig {
    pub input_file_path: String,
    pub input_file_format: Option<String>,
    pub timestamp_column: Option<String>,
    pub key_column: Option<String>,
    pub sink: Option<String>,
    pub sink_url: Option<String>,
    pub topic: Option<String>,
    pub service_name: Option<String>,
    pub speed: Option<f64>,
    pub rate: Option<u64>,
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
    pub speed_multiplier: f64,
    pub rate: Option<u64>,
    pub shift_timestamps: bool,
    pub timestamp_column: String,
    pub key_column: Option<String>,
    pub loop_playback: bool,
    pub advance_loop_timestamps: bool,
    pub loop_id_salt: bool,
    pub progress: bool,
    pub burst: Option<usize>,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            input_file_path: PathBuf::new(),
            input_file_format: None,
            sink: SinkType::Stdout {
                topic: "events".to_string(),
            },
            speed_multiplier: 1.0,
            rate: None,
            shift_timestamps: true,
            timestamp_column: "timestamp".to_string(),
            key_column: None,
            loop_playback: false,
            advance_loop_timestamps: false,
            loop_id_salt: false,
            progress: true,
            burst: None,
        }
    }
}

pub struct ReplayConfigBuilder {
    config: ReplayConfig,
}

impl ReplayConfigBuilder {
    pub fn new() -> Self {
        Self {
            config: ReplayConfig::default(),
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
        self.config.timestamp_column = col.into();
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

    pub fn burst(mut self, burst: usize) -> Self {
        self.config.burst = Some(burst);
        self
    }

    pub fn build(self) -> Result<ReplayConfig, ReplayError> {
        if self.config.input_file_path.as_os_str().is_empty() {
            return Err(ReplayError::Config(
                "Input file path must not be empty".to_string(),
            ));
        }
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

    pub fn from_profile_file(path: &Path) -> Result<Self, ReplayError> {
        let content = fs::read_to_string(path)?;
        let profile: ProfileConfig = toml::from_str(&content)
            .map_err(|e| ReplayError::Config(format!("Failed to parse profile TOML: {e}")))?;

        let topic = profile.topic.unwrap_or_else(|| "events".to_string());
        let sink_type_str = profile.sink.unwrap_or_else(|| "kafka".to_string());
        let sink_url = profile
            .sink_url
            .unwrap_or_else(|| "localhost:9092".to_string());
        let service_name = profile
            .service_name
            .unwrap_or_else(|| "stream_replay_events".to_string());

        let sink = match sink_type_str.to_lowercase().as_str() {
            "kafka" => SinkType::Kafka {
                broker_url: sink_url,
                topic,
            },
            "nats" => SinkType::Nats {
                url: sink_url,
                topic,
            },
            "fluvio" => SinkType::Fluvio {
                url: sink_url,
                topic,
            },
            "iceoryx2" => SinkType::Iceoryx2 { service_name },
            _ => SinkType::Stdout { topic },
        };

        Ok(Self {
            input_file_path: PathBuf::from(profile.input_file_path),
            input_file_format: profile.input_file_format,
            sink,
            speed_multiplier: profile.speed.unwrap_or(1.0),
            rate: profile.rate,
            shift_timestamps: profile.shift_timestamps.unwrap_or(true),
            timestamp_column: profile
                .timestamp_column
                .unwrap_or_else(|| "timestamp".to_string()),
            key_column: profile.key_column,
            loop_playback: profile.loop_playback.unwrap_or(false),
            advance_loop_timestamps: profile.advance_loop_timestamps.unwrap_or(false),
            loop_id_salt: profile.loop_id_salt.unwrap_or(false),
            progress: true,
            burst: None,
        })
    }
}

