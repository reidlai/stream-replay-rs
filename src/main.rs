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

    /// Path to CSV, Parquet, or JSONL dataset file
    #[arg(short, long)]
    pub input_file_path: Option<PathBuf>,

    /// File format: 'csv', 'parquet', or 'jsonl' [default: auto-detect from extension]
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

    /// Playback speed multiplier (e.g. 1.0, 10x, 50x, max) [default: 10x]
    #[arg(long)]
    pub speed: Option<String>,

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

    /// Kanidm authentication service URL
    #[arg(long)]
    pub auth_url: Option<String>,

    /// Kanidm M2M service account ID [default: svc_stream_replay]
    #[arg(long)]
    pub svc_account: Option<String>,

    /// Kanidm M2M service account client secret
    #[arg(long)]
    pub svc_secret: Option<String>,

    /// Kanidm M2M pre-issued bearer token
    #[arg(long)]
    pub svc_token: Option<String>,
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
                if let Some(ref speed_str) = args.speed {
                    match stream_replay_rs::config::parse_speed_preset(speed_str) {
                        Ok(parsed_speed) => {
                            cfg.speed_multiplier = parsed_speed;
                        }
                        Err(e) => {
                            error!("Invalid CLI --speed argument: {e}");
                            exit(1);
                        }
                    }
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
                if let Some(u) = args.auth_url {
                    cfg.auth_url = Some(u);
                }
                if let Some(a) = args.svc_account {
                    cfg.service_account = Some(a);
                }
                if let Some(s) = args.svc_secret {
                    cfg.service_secret = Some(s);
                }
                if let Some(t) = args.svc_token {
                    cfg.auth_token = Some(t);
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

        let mut timestamp_col = args.timestamp_column;
        let mut key_col = args.key_column;

        // Auto-detect Olist schema conventions if not explicitly provided
        if file_path.to_string_lossy().contains("olist") {
            if timestamp_col == "timestamp" {
                timestamp_col = "order_purchase_timestamp".to_string();
            }
            if key_col.is_none() {
                key_col = Some("order_id".to_string());
            }
        }

        // Auto-generate sample olist dataset if missing
        if !file_path.exists()
            && file_path.to_string_lossy().contains("olist")
            && file_path.extension().is_none_or(|ext| ext == "parquet")
        {
            info!(
                "Target Olist dataset {:?} does not exist. Auto-generating 1,000 orders...",
                file_path
            );
            if let Err(e) =
                stream_replay_rs::data_generator::generate_olist_orders_parquet(&file_path, 1000)
            {
                error!("Failed to generate sample Olist dataset: {e}");
            }
        }

        let speed_str = args.speed.as_deref().unwrap_or("10x");
        let speed_multiplier = match stream_replay_rs::config::parse_speed_preset(speed_str) {
            Ok(s) => s,
            Err(e) => {
                error!("Invalid CLI --speed argument: {e}");
                exit(1);
            }
        };

        ReplayConfig {
            input_file_path: file_path,
            input_file_format: args.input_file_format,
            sink,
            speed_multiplier,
            rate: args.rate,
            shift_timestamps: args.shift_timestamps,
            timestamp_column: timestamp_col,
            key_column: key_col,
            loop_playback: args.r#loop,
            advance_loop_timestamps: args.advance_loop_timestamps,
            loop_id_salt: args.loop_id_salt,
            progress: args.progress,
            burst: args.burst,
            auth_url: args.auth_url,
            service_account: args.svc_account,
            service_secret: args.svc_secret,
            auth_token: args.svc_token,
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
                    "Replay finished successfully: {} records dispatched in {:.2?} ({:.1} msgs/s, offsets={}, loops={})",
                    stats.total_records, stats.elapsed, stats.records_per_sec, stats.partition_offsets, stats.loops_completed
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
