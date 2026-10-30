use clap::Parser;
use std::path::PathBuf;
use std::process::exit;
use stream_replay_rs::config::ReplayConfig;
use stream_replay_rs::engine::ReplayEngine;
use stream_replay_rs::sink::SinkType;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(
    name = "stream-replay-rs",
    author = "FerrousCX Contributors",
    version = "0.1.0",
    about = "Generic streaming traffic replay engine and terminal CLI binary"
)]
pub struct CliArgs {
    /// Path to replay profile configuration file (.toml)
    #[arg(short, long)]
    pub config: Option<PathBuf>,

    /// Path to CSV or Parquet dataset file
    #[arg(short, long)]
    pub input_file_path: Option<PathBuf>,

    /// File format: 'csv' or 'parquet' [default: auto-detect from extension]
    #[arg(short = 'f', long)]
    pub input_file_format: Option<String>,

    /// Target streaming sink: 'kafka', 'nats', 'fluvio', 'iceoryx2', or 'stdout'
    #[arg(short, long, default_value = "stdout")]
    pub sink: String,

    /// Target broker connection URL
    #[arg(short = 'u', long, default_value = "localhost:9092")]
    pub sink_url: String,

    /// Destination topic or subject name
    #[arg(short, long, default_value = "events")]
    pub topic: String,

    /// Target iceoryx2 service name (when --sink iceoryx2)
    #[arg(long, default_value = "stream_replay_events")]
    pub service_name: String,

    /// Playback speed multiplier (e.g. 1.0 = real-time, 10.0 = 10x)
    #[arg(long, default_value_t = 1.0)]
    pub speed: f64,

    /// Fixed target injection rate in events/second (overrides speed)
    #[arg(short, long)]
    pub rate: Option<u64>,

    /// Shift relative timestamps anchoring first record to now
    #[arg(long, default_value_t = true)]
    pub shift_timestamps: bool,

    /// Timestamp column name in dataset
    #[arg(long, default_value = "timestamp")]
    pub timestamp_column: String,

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

    /// Display terminal interactive progress bar
    #[arg(long, default_value_t = true)]
    pub progress: bool,

    /// Micro-burst testing: inject N events immediately
    #[arg(long)]
    pub burst: Option<usize>,
}

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let args = CliArgs::parse();

    let config = if let Some(ref profile_path) = args.config {
        match ReplayConfig::from_profile_file(profile_path) {
            Ok(mut cfg) => {
                // Allow CLI arguments to override profile defaults
                if let Some(p) = args.input_file_path {
                    cfg.input_file_path = p;
                }
                if let Some(f) = args.input_file_format {
                    cfg.input_file_format = Some(f);
                }
                if args.speed != 1.0 {
                    cfg.speed_multiplier = args.speed;
                }
                if args.rate.is_some() {
                    cfg.rate = args.rate;
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
                if args.burst.is_some() {
                    cfg.burst = args.burst;
                }
                cfg
            }
            Err(e) => {
                error!("Failed to load configuration profile: {e}");
                exit(1);
            }
        }
    } else {
        let file_path = match args.input_file_path {
            Some(p) => p,
            None => {
                error!("Error: Either --config or --input-file-path must be specified.");
                exit(1);
            }
        };

        let sink = match args.sink.to_lowercase().as_str() {
            "kafka" => SinkType::Kafka {
                broker_url: args.sink_url,
                topic: args.topic,
            },
            "nats" => SinkType::Nats {
                url: args.sink_url,
                topic: args.topic,
            },
            "fluvio" => SinkType::Fluvio {
                url: args.sink_url,
                topic: args.topic,
            },
            "iceoryx2" => SinkType::Iceoryx2 {
                service_name: args.service_name,
            },
            _ => SinkType::Stdout { topic: args.topic },
        };

        ReplayConfig {
            input_file_path: file_path,
            input_file_format: args.input_file_format,
            sink,
            speed_multiplier: args.speed,
            rate: args.rate,
            shift_timestamps: args.shift_timestamps,
            timestamp_column: args.timestamp_column,
            key_column: args.key_column,
            loop_playback: args.r#loop,
            advance_loop_timestamps: args.advance_loop_timestamps,
            loop_id_salt: args.loop_id_salt,
            progress: args.progress,
            burst: args.burst,
        }
    };

    info!(
        "Starting stream-replay-rs: input={:?}, sink={:?}, speed={}x, loop={}",
        config.input_file_path, config.sink, config.speed_multiplier, config.loop_playback
    );

    match ReplayEngine::new(config).await {
        Ok(mut engine) => match engine.run().await {
            Ok(stats) => {
                info!(
                    "Replay finished successfully: {} records dispatched in {:.2?} (loops={})",
                    stats.total_records, stats.elapsed, stats.loops_completed
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

