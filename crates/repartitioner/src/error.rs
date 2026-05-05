use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to read {path}: {source}")]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to write {path}: {source}")]
    WriteFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to parse YAML config {path}: {source}")]
    ParseYaml {
        path: PathBuf,
        #[source]
        source: serde_yaml::Error,
    },

    #[error("failed to serialize JSON metadata: {0}")]
    SerializeJson(#[from] serde_json::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("invalid config: {0}")]
    InvalidConfig(#[from] ConfigValidationError),

    #[error("unsupported Parquet column type for {column}: {data_type}")]
    UnsupportedColumnType { column: String, data_type: String },

    #[error("input path does not exist or is not accessible: {path}")]
    InputPathNotFound { path: PathBuf },

    #[error("no Parquet files found under input path: {path}")]
    NoParquetFiles { path: PathBuf },

    #[error("input row {row_index} is not available in retained record batches")]
    MissingRetainedRow { row_index: usize },

    #[error("unsupported dataset format: {0}")]
    UnsupportedFormat(String),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigValidationError {
    #[error("dataset.input must not be empty")]
    MissingInputPath,

    #[error("partitioning.key_columns must contain at least one non-empty column name")]
    MissingKeyColumns,

    #[error("partitioning.target_partition_size_mb must be greater than zero, got {value}")]
    InvalidTargetPartitionSize { value: u64 },

    #[error("partitioning.min_partitions must be greater than zero, got {value}")]
    InvalidMinPartitionCount { value: usize },

    #[error("partitioning.max_partitions must be greater than zero, got {value}")]
    InvalidMaxPartitionCount { value: usize },

    #[error(
        "partitioning.min_partitions must not be greater than partitioning.max_partitions, got {min} > {max}"
    )]
    MinPartitionsGreaterThanMax { min: usize, max: usize },

    #[error("unsupported partitioning.strategy: {value}")]
    InvalidStrategyName { value: String },
}

pub type Result<T> = std::result::Result<T, Error>;
