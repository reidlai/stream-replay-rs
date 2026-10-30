# stream-replay-rs

> **Pure-Rust Streaming Traffic Simulator & Multi-Sink Replay Engine**  
> Standalone, dataset-agnostic event replayer with microsecond monotonic time shifting, 10x playback acceleration, and interactive micro-burst stress testing.

[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)
[![Rust](https://img.shields.io/badge/rust-1.80%2B-orange.svg)](#-requirements--installation)
[![Tier](https://img.shields.io/badge/DevSecOps-Tier%201%20Shift--Left-brightgreen.svg)](#-tier-1-devsecops--security-gate)

---

## 🏛️ Architecture & C4 Topology

`stream-replay-rs` is a high-performance, single-responsibility traffic simulator designed for the **FerrousCX** streaming architecture. It reads static historical datasets (Apache Parquet and CSV), transforms timestamps dynamically to anchor to the live replay clock, and dispatches events into pluggable message brokers and shared-memory IPC sinks.

```mermaid
flowchart TD
    subgraph Data Sources ["📁 Historical Data Sources"]
        Parquet["Olist Orders Parquet<br/><code>data/olist_orders.parquet</code>"]
        CSV["CSV Event Log<br/><code>events.csv</code>"]
    end

    subgraph Core Engine ["⚙️ stream-replay-rs Engine"]
        Reader["Arrow / Parquet / CSV Streaming Reader<br/>(&lt;35 MB RSS)"]
        TimeEngine["Monotonic Time Shift Engine<br/><code>Δt = t_now + (loop × Δ_dataset)</code>"]
        Salter["Entity ID Salter<br/><code>_L{n} Primary Key Salting</code>"]
        RatePacer["Pacing &amp; Burst Controller<br/><code>10x Speed Default (~1 ord/min)</code>"]
        Auth["Kanidm M2M Authenticator<br/><code>svc_stream_replay</code>"]
    end

    subgraph Pluggable Sinks ["📡 Pluggable Multi-Sink Architecture"]
        KafkaSink["Apache Iggy / Kafka Gateway<br/><code>orders/intake (port 9092)</code>"]
        NATSSink["NATS JetStream<br/><code>events (port 4222)</code>"]
        FluvioSink["Fluvio Broker<br/><code>events (port 9003)</code>"]
        IceoryxSink["Eclipse iceoryx2<br/><code>Zero-Copy Shared Memory IPC</code>"]
        StdoutSink["Terminal Stdout<br/><code>Inspection &amp; Diagnostics</code>"]
    end

    subgraph Downstream Ecosystem ["🌊 FerrousCX Streaming Platform"]
        IggyBroker["Apache Iggy Broker<br/><code>&lt;1ms fsync commit log</code>"]
        Arroyo["Arroyo 2PC Stream Engine<br/><code>Sub-second window aggregations</code>"]
        DeltaLake["Delta Lake ACID Storage<br/><code>orders_raw_events &amp; state</code>"]
    end

    Parquet --> Reader
    CSV --> Reader
    Reader --> TimeEngine
    TimeEngine --> Salter
    Salter --> RatePacer
    Auth -.->|"OAuth2 Token Exchange"| RatePacer

    RatePacer --> KafkaSink
    RatePacer --> NATSSink
    RatePacer --> FluvioSink
    RatePacer --> IceoryxSink
    RatePacer --> StdoutSink

    KafkaSink --> IggyBroker
    IggyBroker --> Arroyo
    Arroyo --> DeltaLake
```

---

## 📦 Requirements & Installation

- **Rust Compiler**: Rust 1.80+ (MSRV)
- **Pre-commit**: (optional, for local git hook enforcement) `pip install pre-commit && pre-commit install`
- **Security Tools**: `cargo install cargo-deny cargo-audit`

---

## ⚡ Key Features

1. **Pure-Rust & Native Binary**: Zero Python or JVM dependencies. Compiles to an optimized standalone binary and an embeddable async Rust library.
2. **Streaming Reader (<35 MB RSS)**: Bounded memory footprint leveraging Apache Arrow vector record batches and chunked CSV iterators (NFR-9).
3. **Monotonic Timestamp Advancement**: Mathematically shifts historical event timestamps forward to the live wall-clock time. Looping iterations continuously advance time without negative deltas or backward jitter:
   $$\text{Timestamp}_{\text{emitted}} = \text{Timestamp}_{\text{record}} + \Delta_{\text{now}} + (\text{loop\_count} \times \Delta_{\text{dataset}})$$
4. **Loop Entity ID Salting**: Appends loop iteration markers (`_L1`, `_L2`) to entity primary keys (e.g. `order_id`) across infinite loops, preventing primary key deduplication collisions in downstream Delta Lake sinks.
5. **Calibrated 10x Default Playback**:
   - The flagship Brazilian Olist e-commerce dataset contains 100,000 orders spanning 730 days (1,051,200 minutes).
   - Historical baseline rate: $\frac{100,000 \text{ orders}}{1,051,200 \text{ minutes}} \approx 0.095 \text{ orders/min}$.
   - Accelerated by **10x**, the steady-state arrival rate is **~1 order/minute** ($\approx 0.95 \text{ orders/min}$), keeping platform intake queues actively populated during live human inspection.
6. **Interactive Micro-Burst Testing**:
   - Supports `--burst <N>` to instantly inject batches (e.g. `--burst 500` orders) without pacing delays in under 1 second (>500 msgs/s), validating backpressure and buffer stability in Apache Iggy and Arroyo (FR-30).
7. **M2M Authentication**: Pre-flight Kanidm OAuth2 token exchange for `svc_stream_replay` (scopes: `stream:produce:intake`, `telemetry:write`) with offline fallback for disconnected development.

---

## 🚀 Quickstart

### 1. Build Native Binary
```bash
cargo build --release
```

### 2. Standalone Quickstart (Isolated Development)

#### Option A: Terminal Stdout Inspection (Zero-Dependency)
Inspect formatted replay events and monotonic time-shifting immediately in your terminal:
```bash
./target/release/stream-replay-rs \
  --input-file-path data/olist_orders.parquet \
  --sink stdout \
  --speed 10x \
  --progress
```

#### Option B: Minimal Standalone Broker (`docker-compose.dev.yml`)
Start an isolated Apache Iggy message broker locally on port 9092 in seconds:
```bash
# 1. Start lightweight dev broker
docker compose -f docker-compose.dev.yml up -d

# 2. Stream historical events to localhost:9092
./target/release/stream-replay-rs \
  --input-file-path data/olist_orders.parquet \
  --sink kafka \
  --sink-url localhost:9092 \
  --topic orders/intake \
  --speed 10x \
  --progress
```

---

### 3. Integrated FerrousCX Platform Quickstart

In the multi-submodule ecosystem, stream directly into the unified **Apache Iggy + Arroyo Stream Engine + Delta Lake** tier:

```bash
# 1. From workspace root, start the streaming platform tier
docker compose -f ../ferrous-streaming/docker-compose.yml up -d

# 2. Launch the replayer targeting the intake stream
./target/release/stream-replay-rs \
  --input-file-path data/olist_orders.parquet \
  --sink kafka \
  --sink-url localhost:9092 \
  --topic orders/intake \
  --speed 10x \
  --progress

# 3. Monitor live pipeline telemetry & tumbling micro-batches:
#    Arroyo Web Console: http://localhost:5115/pipelines
#    Iggy Management:   http://localhost:3000
```

---

### 4. Interactive Micro-Burst Stress Testing
```bash
# Injects 500 events instantaneously (<1s, >500 msgs/s) to evaluate backpressure
./target/release/stream-replay-rs \
  --input-file-path data/olist_orders.parquet \
  --sink kafka \
  --sink-url localhost:9092 \
  --topic orders/intake \
  --burst 500
```

---

### 5. Continuous Looping Simulation with Entity Salting
```bash
# Loops the dataset continuously with monotonic time advancement and ID salting (_L1, _L2)
./target/release/stream-replay-rs \
  --input-file-path data/olist_orders.parquet \
  --loop \
  --advance-loop-timestamps \
  --loop-id-salt \
  --speed 50x
```

---

## 📚 Programmatic Library Usage

Add `stream-replay-rs` to your `Cargo.toml`:

```toml
[dependencies]
stream-replay-rs = { path = "../stream-replay-rs", features = ["full"] }
tokio = { version = "1.40", features = ["full"] }
```

Embed the `ReplayEngine` into your Rust microservice or test harness:

```rust
use stream_replay_rs::config::ReplayConfig;
use stream_replay_rs::engine::ReplayEngine;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ReplayConfig::builder()
        .file_path(PathBuf::from("data/olist_orders.parquet"))
        .file_format("parquet")
        .timestamp_column("order_purchase_timestamp")
        .key_column("order_id")
        .speed_multiplier(10.0) // 10x calibrated speed
        .progress(false)
        .build()?;

    let mut engine = ReplayEngine::new(config).await?;
    let stats = engine.run().await?;
    println!("Emitted {} records in {:.2?}", stats.total_records, stats.elapsed);
    Ok(())
}
```

---

## 🏷️ Cargo Features

| Feature | Description | Default |
|---------|-------------|---------|
| `kafka` | Apache Kafka / Apache Iggy gateway sink driver | No |
| `nats` | NATS JetStream publish driver | No |
| `fluvio` | Fluvio distributed streaming driver | No |
| `iceoryx2` | Eclipse iceoryx2 zero-copy shared memory IPC driver | No |
| `full` | Enables all sink drivers and protocols | No |

---

## 🛠️ CLI Options & Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--config <PATH>` | `-c` | `None` | Path to profile TOML configuration file |
| `--input-file-path <PATH>` | `-i` | `None` | Path to CSV or Parquet dataset file |
| `--input-file-format <FMT>` | `-f` | Auto | File format (`csv` or `parquet`) |
| `--sink <SINK>` | `-s` | `stdout` | Target sink: `kafka`, `nats`, `fluvio`, `iceoryx2`, `stdout` |
| `--sink-url <URL>` | `-u` | `localhost:9092`| Broker connection URL |
| `--topic <TOPIC>` | `-t` | `events` | Target topic or channel name |
| `--service-name <NAME>` | | `stream_replay_events` | iceoryx2 service name |
| `--speed <PRESET>` | | `10x` | Speed multiplier (`1x`, `10x`, `50x`, `max`, or float) |
| `--rate <N>` | `-r` | `None` | Fixed event injection rate (events/second) |
| `--shift-timestamps` | | `true` | Anchor first event timestamp to current wall-clock time |
| `--timestamp-column <COL>` | | `timestamp` | Timestamp column name in dataset |
| `--key-column <COL>` | | `None` | Entity ID/key column name (auto-detected for Olist) |
| `--loop` | | `false` | Loop playback indefinitely upon reaching EOF |
| `--advance-loop-timestamps`| | `false` | Advance timestamps monotonically on subsequent loops |
| `--loop-id-salt` | | `false` | Append `_L{n}` salt to entity IDs on loop iterations |
| `--progress` | | `true` | Display interactive progress bar |
| `--burst <N>` | | `None` | Inject micro-burst of N events immediately without pacing |
| `--auth-url <URL>` | | `None` | Kanidm authentication server URL |
| `--svc-account <ID>` | | `svc_stream_replay` | Kanidm M2M service account ID |
| `--svc-secret <SECRET>` | | `None` | Kanidm client secret for token exchange |
| `--svc-token <TOKEN>` | | `None` | Pre-issued Kanidm bearer token |

---

## ⚙️ Profile Configuration (`profile.toml`)

Reusable replay scenarios can be defined in TOML profiles:

```toml
input_file_path = "data/olist_orders.parquet"
sink = "kafka"
sink_url = "localhost:9092"
topic = "orders/intake"
speed = "10x"
shift_timestamps = true
timestamp_column = "order_purchase_timestamp"
key_column = "order_id"
loop_playback = true
advance_loop_timestamps = true
loop_id_salt = true

# Kanidm M2M Credentials
auth_url = "https://localhost:8443"
service_account = "svc_stream_replay"
service_secret = "m2m_stream_replay_secret_token"
```

Execute via profile:
```bash
stream-replay-rs --config profile.toml
```

---

## 🔒 Tier 1 DevSecOps & Security Gate

In accordance with architectural decision **AD-13**, `stream-replay-rs` adheres to shift-left DevSecOps standards enforced entirely on local developer workstations with zero external GitHub Actions dependencies.

### Quality & Security Controls:
- **Secret Detection**: `gitleaks` pre-commit scanning.
- **Code Hygiene**: Strict `cargo fmt` and `cargo clippy --all-targets -- -D warnings`.
- **License & Dependency Audits**: `cargo-deny` and `cargo-audit` policy verification.
- **Performance & Testing**: 100% of unit and integration tests execute in <10 seconds.

### Dual-Shell Audit Scripts:

Both scripts provide complete cross-platform operational parity and return standard exit codes:

```bash
# Linux / macOS / WSL
./scripts/audit-and-scan.sh
```

```powershell
# Windows PowerShell
pwsh ./scripts/audit-and-scan.ps1
```

---

## 📄 License

Dual-licensed under the **MIT License** and **Apache License 2.0**.
