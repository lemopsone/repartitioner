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

    #[error("CSV error: {0}")]
    Csv(#[from] csv::Error),

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

    #[error(
        "resource limit exceeded: estimated dataset size {estimated_dataset_size_mb} MB exceeds configured memory limit {configured_memory_limit_mb} MB"
    )]
    ResourceLimitExceeded {
        configured_memory_limit_mb: u64,
        estimated_dataset_size_mb: u64,
    },
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ConfigValidationError {
    #[error("dataset.input must not be empty")]
    MissingInputPath,

    #[error("dataset.input_format or dataset.format must be specified")]
    MissingInputFormat,

    #[error("output.path or dataset.output must be specified")]
    MissingOutputPath,

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

    #[error(
        "partitioning.no_op_max_imbalance_ratio must be finite and greater than zero, got {value}"
    )]
    InvalidNoOpMaxImbalanceRatio { value: f64 },

    #[error("statistics.approximate_capacity must be greater than zero, got {value}")]
    InvalidApproximateCapacity { value: usize },

    #[error("join.right_input must not be empty")]
    MissingJoinRightInput,

    #[error("join.join_keys must contain at least one non-empty column name")]
    MissingJoinKeys,

    #[error("storage.target_file_size_mb must be greater than zero, got {value}")]
    InvalidTargetFileSize { value: u64 },

    #[error("storage.min_file_size_mb must be greater than zero, got {value}")]
    InvalidMinFileSize { value: u64 },

    #[error(
        "storage.min_file_size_mb must not be greater than storage.target_file_size_mb, got {min} > {target}"
    )]
    MinFileSizeGreaterThanTarget { min: u64, target: u64 },

    #[error("unsupported partitioning.strategy: {value}")]
    InvalidStrategyName { value: String },
}

pub type Result<T> = std::result::Result<T, Error>;
