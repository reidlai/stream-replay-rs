use thiserror::Error;

#[derive(Error, Debug)]
pub enum ReplayError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("CSV parsing error: {0}")]
    Csv(#[from] csv::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Timestamp parsing error: {0}")]
    TimestampParse(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Sink dispatch error: {0}")]
    Sink(String),

    #[error("Dataset error: {0}")]
    Dataset(String),
}
