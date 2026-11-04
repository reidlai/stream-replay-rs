use clap::Parser;
use std::path::PathBuf;
use std::process::exit;
use stream_replay_rs::config::{parse_speed_preset, sink_from_parts, sink_parts, ReplayConfig};
use stream_replay_rs::engine::ReplayEngine;
use stream_replay_rs::error::ReplayError;
use stream_replay_rs::time_shift::{EventTimeMode, TimestampUnit};
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(name = "stream-replay-rs", version, about)]
pub struct CliArgs {
    /// Path to a replay profile (.toml); command-line flags override it
    #[arg(short, long)]
    pub config: Option<PathBuf>,

    /// Path to CSV, Parquet, or JSONL dataset file
    #[arg(short, long)]
    pub input_file_path: Option<PathBuf>,

    /// File format: 'csv', 'parquet', or 'jsonl' [default: auto-detect from extension]
    #[arg(short = 'f', long)]
    pub input_file_format: Option<String>,

    /// Target sink: 'kafka' or 'stdout' [default: stdout]
    #[arg(short, long)]
    pub sink: Option<String>,

    /// Kafka-compatible endpoint, host:port [default: localhost:9092]
    #[arg(short = 'u', long)]
    pub sink_url: Option<String>,

    /// Destination topic; it must already exist [default: events]
    #[arg(short, long)]
    pub topic: Option<String>,

    /// Roll the dataset forward N times faster than its original spacing
    /// (1, 2.5, 10x, or max for no pacing) [default: 1.0]
    #[arg(long, conflicts_with = "rate")]
    pub speed: Option<String>,

    /// Fixed events per second, ignoring the dataset's own spacing
    #[arg(short, long, value_parser = clap::value_parser!(u64).range(1..))]
    pub rate: Option<u64>,

    /// Pacing column that drives the emit schedule [default: auto-detect]
    #[arg(long)]
    pub timestamp_column: Option<String>,

    /// Unit of numeric timestamps: auto, s, ms, us, ns or rfc3339 [default: auto]
    #[arg(long, value_enum)]
    pub timestamp_unit: Option<TimestampUnit>,

    /// Another timestamp column to rebase by the same offset (repeatable)
    #[arg(long = "shift-column", value_name = "COL")]
    pub shift_column: Vec<String>,

    /// Rewrite payload timestamps onto the live clock [default]
    #[arg(long, overrides_with = "no_shift_timestamps")]
    pub shift_timestamps: bool,

    /// Pass payload timestamps through unchanged; pacing still applies
    #[arg(long, overrides_with = "shift_timestamps")]
    pub no_shift_timestamps: bool,

    /// Whether rewritten payload timestamps follow the compressed schedule or keep the
    /// original spacing when the speed is not 1 [default: compress]
    #[arg(long, value_enum)]
    pub event_time: Option<EventTimeMode>,

    /// Key/ID column name in dataset
    #[arg(long)]
    pub key_column: Option<String>,

    /// Loop replay indefinitely upon reaching EOF
    #[arg(long, default_value_t = false)]
    pub r#loop: bool,

    /// Monotonically advance timestamps on subsequent loop iterations
    #[arg(long, default_value_t = false)]
    pub advance_loop_timestamps: bool,

    /// Append loop counter salt (_L1, _L2) to primary keys to prevent collisions
    #[arg(long, default_value_t = false)]
    pub loop_id_salt: bool,

    /// Show the progress bar [default]
    #[arg(long, overrides_with = "no_progress")]
    pub progress: bool,

    /// Hide the progress bar
    #[arg(long, overrides_with = "progress")]
    pub no_progress: bool,
}

/// Resolve the effective configuration: the profile (if any), then command-line overrides.
pub fn build_config(args: CliArgs) -> Result<ReplayConfig, ReplayError> {
    let mut cfg = match args.config {
        Some(ref profile_path) => ReplayConfig::from_profile_file(profile_path)?,
        None => ReplayConfig::default(),
    };

    if let Some(p) = args.input_file_path {
        cfg.input_file_path = p;
    }
    if cfg.input_file_path.as_os_str().is_empty() {
        return Err(ReplayError::Config(
            "Either --config (with an input path) or --input-file-path must be specified."
                .to_string(),
        ));
    }
    if let Some(f) = args.input_file_format {
        cfg.input_file_format = Some(f);
    }

    if args.sink.is_some() || args.sink_url.is_some() || args.topic.is_some() {
        let (kind, url, topic) = sink_parts(&cfg.sink);
        cfg.sink = sink_from_parts(
            &args.sink.unwrap_or(kind),
            args.sink_url.unwrap_or(url),
            args.topic.unwrap_or(topic),
        )?;
    }

    if let Some(ref speed) = args.speed {
        cfg.speed_multiplier = parse_speed_preset(speed)?;
        cfg.rate = None;
    }
    if let Some(rate) = args.rate {
        cfg.rate = Some(rate);
    }

    if let Some(c) = args.timestamp_column {
        cfg.timestamp_column = Some(c);
    }
    if let Some(unit) = args.timestamp_unit {
        cfg.timestamp_unit = unit;
    }
    if !args.shift_column.is_empty() {
        cfg.shift_columns = args.shift_column;
    }
    if args.no_shift_timestamps {
        cfg.shift_timestamps = false;
    } else if args.shift_timestamps {
        cfg.shift_timestamps = true;
    }
    if let Some(mode) = args.event_time {
        cfg.event_time = mode;
    }
    if let Some(k) = args.key_column {
        cfg.key_column = Some(k);
    }

    if args.r#loop {
        cfg.loop_playback = true;
    }
    if args.advance_loop_timestamps {
        cfg.advance_loop_timestamps = true;
    }
    if args.loop_id_salt {
        cfg.loop_id_salt = true;
    }
    if args.no_progress {
        cfg.progress = false;
    } else if args.progress {
        cfg.progress = true;
    }

    cfg.validate()?;
    Ok(cfg)
}

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let config = match build_config(CliArgs::parse()) {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("Invalid configuration: {e}");
            exit(1);
        }
    };

    let pacing = match config.rate {
        Some(rate) => format!("rate={rate}/s"),
        None => format!("speed={}x", config.speed_multiplier),
    };
    info!(
        "Starting stream-replay-rs: input={:?}, sink={:?}, {}, loop={}",
        config.input_file_path, config.sink, pacing, config.loop_playback
    );

    match ReplayEngine::new(config).await {
        Ok(mut engine) => match engine.run().await {
            Ok(stats) => {
                info!(
                    "Replay finished successfully: {} records dispatched in {:.2?} ({:.1} msgs/s, loops={})",
                    stats.total_records, stats.elapsed, stats.records_per_sec, stats.loops_completed
                );
            }
            Err(e) => {
                error!("Replay runtime error: {e}");
                exit(1);
            }
        },
        Err(e) => {
            error!("Failed to initialize ReplayEngine: {e}");
            exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::error::ErrorKind;
    use stream_replay_rs::sink::SinkType;

    fn parse(extra: &[&str]) -> Result<CliArgs, clap::Error> {
        let mut argv = vec!["stream-replay-rs", "--input-file-path", "events.csv"];
        argv.extend_from_slice(extra);
        CliArgs::try_parse_from(argv)
    }

    #[test]
    fn default_speed_is_one_with_original_spacing() {
        let cfg = build_config(parse(&[]).unwrap()).unwrap();
        assert_eq!(cfg.speed_multiplier, 1.0);
        assert_eq!(cfg.rate, None);
        assert_eq!(cfg.event_time, EventTimeMode::Compress);
        assert_eq!(cfg.timestamp_unit, TimestampUnit::Auto);
        assert!(cfg.shift_timestamps);
        assert!(cfg.shift_columns.is_empty());
        assert!(cfg.timestamp_column.is_none());
    }

    #[test]
    fn speed_formats_are_accepted() {
        for (raw, expected) in [("10x", 10.0), ("10", 10.0), ("2.5", 2.5), ("max", 0.0)] {
            let cfg = build_config(parse(&["--speed", raw]).unwrap()).unwrap();
            assert_eq!(cfg.speed_multiplier, expected, "--speed {raw}");
        }
        assert!(build_config(parse(&["--speed", "fast"]).unwrap()).is_err());
    }

    #[test]
    fn speed_and_rate_are_mutually_exclusive() {
        let err = parse(&["--speed", "10x", "--rate", "25"]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ArgumentConflict);
        let err = parse(&["--rate", "25", "--speed", "10x"]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ArgumentConflict);

        let cfg = build_config(parse(&["--rate", "25"]).unwrap()).unwrap();
        assert_eq!(cfg.rate, Some(25));
        assert!(parse(&["--rate", "0"]).is_err());
    }

    #[test]
    fn new_timestamp_flags_are_parsed() {
        let cfg = build_config(
            parse(&[
                "--event-time",
                "preserve",
                "--timestamp-unit",
                "ms",
                "--timestamp-column",
                "event_ts",
                "--shift-column",
                "ready_ts",
                "--shift-column",
                "done_ts",
                "--no-shift-timestamps",
            ])
            .unwrap(),
        )
        .unwrap();
        assert_eq!(cfg.event_time, EventTimeMode::Preserve);
        assert_eq!(cfg.timestamp_unit, TimestampUnit::Millis);
        assert_eq!(cfg.timestamp_column.as_deref(), Some("event_ts"));
        assert_eq!(cfg.shift_columns, vec!["ready_ts", "done_ts"]);
        assert!(!cfg.shift_timestamps);
        assert!(parse(&["--timestamp-unit", "minutes"]).is_err());
        assert!(parse(&["--event-time", "stretch"]).is_err());
    }

    #[test]
    fn removed_flags_are_rejected() {
        for flag in [
            "--burst",
            "--auth-url",
            "--svc-account",
            "--svc-secret",
            "--svc-token",
        ] {
            let err = parse(&[flag, "1"]).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::UnknownArgument, "{flag}");
        }
    }

    #[test]
    fn sink_flags_default_to_stdout_and_override_independently() {
        let cfg = build_config(parse(&[]).unwrap()).unwrap();
        assert_eq!(
            cfg.sink,
            SinkType::Stdout {
                topic: "events".to_string()
            }
        );

        let cfg = build_config(
            parse(&[
                "--sink",
                "kafka",
                "--sink-url",
                "broker:9092",
                "--topic",
                "t",
            ])
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            cfg.sink,
            SinkType::Kafka {
                broker_url: "broker:9092".to_string(),
                topic: "t".to_string()
            }
        );
    }

    #[test]
    fn missing_input_is_an_error() {
        let args = CliArgs::try_parse_from(["stream-replay-rs"]).unwrap();
        assert!(build_config(args).is_err());
    }
}
