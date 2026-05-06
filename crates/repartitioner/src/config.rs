use std::{
    fs,
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    str::FromStr,
};

use serde::{Deserialize, Serialize};

use crate::{error::ConfigValidationError, Error, Result};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Config {
    pub dataset: DatasetConfig,
    pub partitioning: PartitioningConfig,
    pub statistics: StatisticsConfig,
    pub storage: StorageConfig,
    pub output: OutputConfig,
    pub job: JobConfig,
    pub join: Option<JoinConfig>,
    pub resources: ResourceConfig,
}

impl Config {
    pub fn from_yaml_str(input: &str) -> Result<Self> {
        let raw: RawConfig = serde_yaml::from_str(input).map_err(|source| Error::ParseYaml {
            path: PathBuf::from("<inline>"),
            source,
        })?;

        raw.try_into().map_err(Error::from)
    }

    pub fn from_yaml_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path).map_err(|source| Error::ReadFile {
            path: path.to_path_buf(),
            source,
        })?;

        let raw: RawConfig =
            serde_yaml::from_str(&contents).map_err(|source| Error::ParseYaml {
                path: path.to_path_buf(),
                source,
            })?;

        raw.try_into().map_err(Error::from)
    }

    pub fn validate(&self) -> std::result::Result<(), ConfigValidationError> {
        if self.dataset.input.as_os_str().is_empty() {
            return Err(ConfigValidationError::MissingInputPath);
        }

        if self.output.path.as_os_str().is_empty() {
            return Err(ConfigValidationError::MissingOutputPath);
        }

        if self
            .partitioning
            .key_columns
            .iter()
            .all(|column| column.trim().is_empty())
        {
            return Err(ConfigValidationError::MissingKeyColumns);
        }

        if self.partitioning.min_partitions > self.partitioning.max_partitions {
            return Err(ConfigValidationError::MinPartitionsGreaterThanMax {
                min: self.partitioning.min_partitions.get(),
                max: self.partitioning.max_partitions.get(),
            });
        }

        if !self.partitioning.no_op_max_imbalance_ratio.is_finite()
            || self.partitioning.no_op_max_imbalance_ratio <= 0.0
        {
            return Err(ConfigValidationError::InvalidNoOpMaxImbalanceRatio {
                value: self.partitioning.no_op_max_imbalance_ratio,
            });
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DatasetConfig {
    pub input: PathBuf,
    pub input_format: DatasetFormat,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatasetFormat {
    Parquet,
    Csv,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PartitioningConfig {
    pub key_columns: Vec<String>,
    pub min_partitions: NonZeroUsize,
    pub target_partition_size_mb: NonZeroU64,
    pub max_partitions: NonZeroUsize,
    pub strategy: PartitioningStrategy,
    pub normal_key_assignment: NormalKeyAssignment,
    pub heavy_key_alpha: f64,
    pub force_rewrite: bool,
    pub no_op_max_imbalance_ratio: f64,
    pub seed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StatisticsConfig {
    pub heavy_hitter_mode: HeavyHitterMode,
    pub approximate_capacity: NonZeroUsize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StorageConfig {
    pub target_file_size_mb: NonZeroU64,
    pub min_file_size_mb: NonZeroU64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HeavyHitterMode {
    Exact,
    Approximate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputConfig {
    pub path: PathBuf,
    pub format: DatasetFormat,
    pub include_technical_columns: bool,
    pub partition_column: String,
    pub salt_column: String,
    pub heavy_key_column: String,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("./data/output_partitioned"),
            format: DatasetFormat::Parquet,
            include_technical_columns: true,
            partition_column: "_rp_partition_id".to_string(),
            salt_column: "_rp_salt".to_string(),
            heavy_key_column: "_rp_is_heavy_key".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitioningStrategy {
    AdaptiveHashSalt,
    FileSizeBalancing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NormalKeyAssignment {
    Hash,
    LoadAware,
}

impl FromStr for PartitioningStrategy {
    type Err = ConfigValidationError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "adaptive_hash_salt" => Ok(Self::AdaptiveHashSalt),
            "file_size_balancing" => Ok(Self::FileSizeBalancing),
            _ => Err(ConfigValidationError::InvalidStrategyName {
                value: value.to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobConfig {
    #[serde(rename = "type")]
    pub job_type: JobType,
    pub downstream_engine: DownstreamEngine,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JoinConfig {
    pub left_input: PathBuf,
    pub right_input: PathBuf,
    pub join_keys: Vec<String>,
    pub right_side_mode: RightSideMode,
    pub broadcast_threshold_mb: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RightSideMode {
    BroadcastIfSmall,
    Shuffle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobType {
    Scan,
    GroupBy,
    Join,
    Filter,
    Generic,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DownstreamEngine {
    Spark,
    Generic,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceConfig {
    pub local_threads: usize,
    pub memory_limit_mb: u64,
    pub fail_on_memory_limit: bool,
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    dataset: RawDatasetConfig,
    partitioning: RawPartitioningConfig,
    statistics: Option<RawStatisticsConfig>,
    storage: Option<RawStorageConfig>,
    output: Option<RawOutputConfig>,
    job: JobConfig,
    join: Option<RawJoinConfig>,
    resources: RawResourceConfig,
}

#[derive(Debug, Deserialize)]
struct RawDatasetConfig {
    input: String,
    output: Option<String>,
    format: Option<DatasetFormat>,
    input_format: Option<DatasetFormat>,
}

#[derive(Debug, Deserialize)]
struct RawPartitioningConfig {
    key_columns: Vec<String>,
    min_partitions: Option<usize>,
    target_partition_size_mb: u64,
    max_partitions: usize,
    strategy: String,
    normal_key_assignment: Option<NormalKeyAssignment>,
    heavy_key_alpha: f64,
    force_rewrite: Option<bool>,
    no_op_max_imbalance_ratio: Option<f64>,
    seed: u64,
}

#[derive(Debug, Deserialize)]
struct RawStatisticsConfig {
    heavy_hitter_mode: Option<HeavyHitterMode>,
    approximate_capacity: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct RawStorageConfig {
    target_file_size_mb: Option<u64>,
    min_file_size_mb: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct RawOutputConfig {
    path: Option<String>,
    format: Option<DatasetFormat>,
    include_technical_columns: Option<bool>,
    partition_column: Option<String>,
    salt_column: Option<String>,
    heavy_key_column: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawJoinConfig {
    left_input: Option<String>,
    right_input: String,
    join_keys: Vec<String>,
    right_side_mode: Option<RightSideMode>,
    broadcast_threshold_mb: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct RawResourceConfig {
    local_threads: usize,
    memory_limit_mb: u64,
    fail_on_memory_limit: Option<bool>,
}

impl TryFrom<RawConfig> for Config {
    type Error = ConfigValidationError;

    fn try_from(raw: RawConfig) -> std::result::Result<Self, Self::Error> {
        if raw.dataset.input.trim().is_empty() {
            return Err(ConfigValidationError::MissingInputPath);
        }

        if raw
            .partitioning
            .key_columns
            .iter()
            .all(|column| column.trim().is_empty())
        {
            return Err(ConfigValidationError::MissingKeyColumns);
        }

        let target_partition_size_mb = NonZeroU64::new(raw.partitioning.target_partition_size_mb)
            .ok_or(
            ConfigValidationError::InvalidTargetPartitionSize {
                value: raw.partitioning.target_partition_size_mb,
            },
        )?;

        let raw_min_partitions = raw.partitioning.min_partitions.unwrap_or(1);
        let min_partitions = NonZeroUsize::new(raw_min_partitions).ok_or(
            ConfigValidationError::InvalidMinPartitionCount {
                value: raw_min_partitions,
            },
        )?;

        let max_partitions = NonZeroUsize::new(raw.partitioning.max_partitions).ok_or(
            ConfigValidationError::InvalidMaxPartitionCount {
                value: raw.partitioning.max_partitions,
            },
        )?;

        if min_partitions.get() > max_partitions.get() {
            return Err(ConfigValidationError::MinPartitionsGreaterThanMax {
                min: min_partitions.get(),
                max: max_partitions.get(),
            });
        }

        let no_op_max_imbalance_ratio = raw.partitioning.no_op_max_imbalance_ratio.unwrap_or(1.2);
        if !no_op_max_imbalance_ratio.is_finite() || no_op_max_imbalance_ratio <= 0.0 {
            return Err(ConfigValidationError::InvalidNoOpMaxImbalanceRatio {
                value: no_op_max_imbalance_ratio,
            });
        }

        let strategy = PartitioningStrategy::from_str(raw.partitioning.strategy.as_str())?;
        let statistics = statistics_config(raw.statistics)?;
        let storage = storage_config(raw.storage)?;
        let legacy_dataset_format = raw.dataset.format.clone();
        let input_format = raw
            .dataset
            .input_format
            .clone()
            .or_else(|| legacy_dataset_format.clone())
            .ok_or(ConfigValidationError::MissingInputFormat)?;
        let output = output_config(raw.output, raw.dataset.output, legacy_dataset_format)?;

        let config = Config {
            dataset: DatasetConfig {
                input: PathBuf::from(raw.dataset.input.clone()),
                input_format,
            },
            partitioning: PartitioningConfig {
                key_columns: raw.partitioning.key_columns,
                min_partitions,
                target_partition_size_mb,
                max_partitions,
                strategy,
                normal_key_assignment: raw
                    .partitioning
                    .normal_key_assignment
                    .unwrap_or(NormalKeyAssignment::LoadAware),
                heavy_key_alpha: raw.partitioning.heavy_key_alpha,
                force_rewrite: raw.partitioning.force_rewrite.unwrap_or(false),
                no_op_max_imbalance_ratio,
                seed: raw.partitioning.seed,
            },
            statistics,
            storage,
            output,
            job: raw.job,
            join: join_config(raw.join, &raw.dataset.input)?,
            resources: ResourceConfig {
                local_threads: raw.resources.local_threads,
                memory_limit_mb: raw.resources.memory_limit_mb,
                fail_on_memory_limit: raw.resources.fail_on_memory_limit.unwrap_or(false),
            },
        };

        config.validate()?;
        Ok(config)
    }
}

fn statistics_config(
    raw: Option<RawStatisticsConfig>,
) -> std::result::Result<StatisticsConfig, ConfigValidationError> {
    let raw = raw.unwrap_or(RawStatisticsConfig {
        heavy_hitter_mode: None,
        approximate_capacity: None,
    });
    let capacity = raw.approximate_capacity.unwrap_or(10_000);
    let approximate_capacity = NonZeroUsize::new(capacity)
        .ok_or(ConfigValidationError::InvalidApproximateCapacity { value: capacity })?;

    Ok(StatisticsConfig {
        heavy_hitter_mode: raw.heavy_hitter_mode.unwrap_or(HeavyHitterMode::Exact),
        approximate_capacity,
    })
}

fn join_config(
    raw: Option<RawJoinConfig>,
    default_left_input: &str,
) -> std::result::Result<Option<JoinConfig>, ConfigValidationError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    if raw.right_input.trim().is_empty() {
        return Err(ConfigValidationError::MissingJoinRightInput);
    }
    if raw.join_keys.iter().all(|key| key.trim().is_empty()) {
        return Err(ConfigValidationError::MissingJoinKeys);
    }

    Ok(Some(JoinConfig {
        left_input: PathBuf::from(
            raw.left_input
                .unwrap_or_else(|| default_left_input.to_string()),
        ),
        right_input: PathBuf::from(raw.right_input),
        join_keys: raw.join_keys,
        right_side_mode: raw
            .right_side_mode
            .unwrap_or(RightSideMode::BroadcastIfSmall),
        broadcast_threshold_mb: raw.broadcast_threshold_mb.unwrap_or(10),
    }))
}

fn storage_config(
    raw: Option<RawStorageConfig>,
) -> std::result::Result<StorageConfig, ConfigValidationError> {
    let raw = raw.unwrap_or(RawStorageConfig {
        target_file_size_mb: None,
        min_file_size_mb: None,
    });
    let target = raw.target_file_size_mb.unwrap_or(128);
    let min = raw.min_file_size_mb.unwrap_or(16);
    let target_file_size_mb = NonZeroU64::new(target)
        .ok_or(ConfigValidationError::InvalidTargetFileSize { value: target })?;
    let min_file_size_mb =
        NonZeroU64::new(min).ok_or(ConfigValidationError::InvalidMinFileSize { value: min })?;

    if min_file_size_mb > target_file_size_mb {
        return Err(ConfigValidationError::MinFileSizeGreaterThanTarget { min, target });
    }

    Ok(StorageConfig {
        target_file_size_mb,
        min_file_size_mb,
    })
}

fn output_config(
    raw: Option<RawOutputConfig>,
    legacy_dataset_output: Option<String>,
    legacy_dataset_format: Option<DatasetFormat>,
) -> std::result::Result<OutputConfig, ConfigValidationError> {
    let defaults = OutputConfig::default();
    match raw {
        Some(raw) => {
            let path = raw
                .path
                .or(legacy_dataset_output)
                .ok_or(ConfigValidationError::MissingOutputPath)?;
            if path.trim().is_empty() {
                return Err(ConfigValidationError::MissingOutputPath);
            }
            Ok(OutputConfig {
                path: PathBuf::from(path),
                format: raw
                    .format
                    .or(legacy_dataset_format)
                    .unwrap_or_else(|| defaults.format.clone()),
                include_technical_columns: raw
                    .include_technical_columns
                    .unwrap_or(defaults.include_technical_columns),
                partition_column: raw.partition_column.unwrap_or(defaults.partition_column),
                salt_column: raw.salt_column.unwrap_or(defaults.salt_column),
                heavy_key_column: raw.heavy_key_column.unwrap_or(defaults.heavy_key_column),
            })
        }
        None => {
            let path = legacy_dataset_output.ok_or(ConfigValidationError::MissingOutputPath)?;
            if path.trim().is_empty() {
                return Err(ConfigValidationError::MissingOutputPath);
            }
            Ok(OutputConfig {
                path: PathBuf::from(path),
                format: legacy_dataset_format.unwrap_or_else(|| defaults.format.clone()),
                ..defaults
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE_CONFIG: &str = r#"
dataset:
  input: "./data/input.parquet"
  output: "./data/output_partitioned"
  format: "parquet"

partitioning:
  key_columns: ["user_id"]
  target_partition_size_mb: 128
  max_partitions: 128
  strategy: "adaptive_hash_salt"
  heavy_key_alpha: 2.0
  seed: 42

job:
  type: "group_by"
  downstream_engine: "spark"

resources:
  local_threads: 8
  memory_limit_mb: 4096
"#;

    #[test]
    fn parses_yaml_config() {
        let config = Config::from_yaml_str(EXAMPLE_CONFIG).expect("config should parse");

        assert_eq!(config.dataset.input, PathBuf::from("./data/input.parquet"));
        assert_eq!(config.dataset.input_format, DatasetFormat::Parquet);
        assert_eq!(config.partitioning.key_columns, vec!["user_id".to_string()]);
        assert_eq!(config.partitioning.min_partitions.get(), 1);
        assert_eq!(config.partitioning.target_partition_size_mb.get(), 128);
        assert_eq!(config.partitioning.max_partitions.get(), 128);
        assert!(!config.partitioning.force_rewrite);
        assert_eq!(config.partitioning.no_op_max_imbalance_ratio, 1.2);
        assert_eq!(config.statistics.heavy_hitter_mode, HeavyHitterMode::Exact);
        assert_eq!(config.statistics.approximate_capacity.get(), 10_000);
        assert_eq!(config.storage.target_file_size_mb.get(), 128);
        assert_eq!(config.storage.min_file_size_mb.get(), 16);
        assert_eq!(
            config.output.path,
            PathBuf::from("./data/output_partitioned")
        );
        assert_eq!(config.output.format, DatasetFormat::Parquet);
        assert!(config.output.include_technical_columns);
        assert_eq!(config.output.partition_column, "_rp_partition_id");
        assert_eq!(config.output.salt_column, "_rp_salt");
        assert_eq!(config.output.heavy_key_column, "_rp_is_heavy_key");
        assert_eq!(
            config.partitioning.strategy,
            PartitioningStrategy::AdaptiveHashSalt
        );
        assert_eq!(
            config.partitioning.normal_key_assignment,
            NormalKeyAssignment::LoadAware
        );
        assert_eq!(config.job.job_type, JobType::GroupBy);
        assert_eq!(config.job.downstream_engine, DownstreamEngine::Spark);
        assert!(config.join.is_none());
        assert_eq!(config.resources.local_threads, 8);
        assert_eq!(config.resources.memory_limit_mb, 4096);
        assert!(!config.resources.fail_on_memory_limit);
    }

    #[test]
    fn old_dataset_format_config_still_parses() {
        let config = Config::from_yaml_str(EXAMPLE_CONFIG).expect("old config should parse");

        assert_eq!(config.dataset.input_format, DatasetFormat::Parquet);
        assert_eq!(
            config.output.path,
            PathBuf::from("./data/output_partitioned")
        );
        assert_eq!(config.output.format, DatasetFormat::Parquet);
    }

    #[test]
    fn parquet_input_parquet_output_unchanged() {
        let config = Config::from_yaml_str(EXAMPLE_CONFIG).expect("parquet config should parse");

        assert_eq!(config.dataset.input, PathBuf::from("./data/input.parquet"));
        assert_eq!(config.dataset.input_format, DatasetFormat::Parquet);
        assert_eq!(
            config.output.path,
            PathBuf::from("./data/output_partitioned")
        );
        assert_eq!(config.output.format, DatasetFormat::Parquet);
    }

    #[test]
    fn new_input_output_format_config_parses() {
        let config = Config::from_yaml_str(
            r#"
dataset:
  input: "./data/input.csv"
  input_format: "csv"

output:
  path: "./data/output_partitioned"
  format: "parquet"
  include_technical_columns: true
  partition_column: "_rp_partition_id"
  salt_column: "_rp_salt"
  heavy_key_column: "_rp_is_heavy_key"

partitioning:
  key_columns: ["user_id"]
  target_partition_size_mb: 128
  max_partitions: 128
  strategy: "adaptive_hash_salt"
  heavy_key_alpha: 2.0
  seed: 42

job:
  type: "group_by"
  downstream_engine: "spark"

resources:
  local_threads: 8
  memory_limit_mb: 4096
"#,
        )
        .expect("new config should parse");

        assert_eq!(config.dataset.input, PathBuf::from("./data/input.csv"));
        assert_eq!(config.dataset.input_format, DatasetFormat::Csv);
        assert_eq!(
            config.output.path,
            PathBuf::from("./data/output_partitioned")
        );
        assert_eq!(config.output.format, DatasetFormat::Parquet);
    }

    #[test]
    fn parses_csv_dataset_format_as_legacy_input_and_output_format() {
        let config = parse_replacing("format: \"parquet\"", "format: \"csv\"")
            .expect("csv config should parse");

        assert_eq!(config.dataset.input_format, DatasetFormat::Csv);
        assert_eq!(config.output.format, DatasetFormat::Csv);
    }

    #[test]
    fn rejects_missing_input_path() {
        let error = parse_replacing("input: \"./data/input.parquet\"", "input: \"\"")
            .expect_err("empty input should be rejected");
        assert_validation_error(error, ConfigValidationError::MissingInputPath);
    }

    #[test]
    fn rejects_missing_key_columns() {
        let error = parse_replacing("key_columns: [\"user_id\"]", "key_columns: []")
            .expect_err("empty keys should be rejected");
        assert_validation_error(error, ConfigValidationError::MissingKeyColumns);
    }

    #[test]
    fn rejects_invalid_target_partition_size() {
        let error = parse_replacing(
            "target_partition_size_mb: 128",
            "target_partition_size_mb: 0",
        )
        .expect_err("zero target size should be rejected");
        assert_validation_error(
            error,
            ConfigValidationError::InvalidTargetPartitionSize { value: 0 },
        );
    }

    #[test]
    fn rejects_invalid_max_partition_count() {
        let error = parse_replacing("max_partitions: 128", "max_partitions: 0")
            .expect_err("zero partitions should be rejected");
        assert_validation_error(
            error,
            ConfigValidationError::InvalidMaxPartitionCount { value: 0 },
        );
    }

    #[test]
    fn parses_optional_min_partition_count() {
        let config = parse_replacing(
            "max_partitions: 128",
            "min_partitions: 2\n  max_partitions: 128",
        )
        .expect("config with min_partitions should parse");

        assert_eq!(config.partitioning.min_partitions.get(), 2);
    }

    #[test]
    fn parses_optional_rewrite_controls() {
        let config = parse_replacing(
            "heavy_key_alpha: 2.0",
            "heavy_key_alpha: 2.0\n  force_rewrite: true\n  no_op_max_imbalance_ratio: 1.5",
        )
        .expect("config with rewrite controls should parse");

        assert!(config.partitioning.force_rewrite);
        assert_eq!(config.partitioning.no_op_max_imbalance_ratio, 1.5);
    }

    #[test]
    fn parses_optional_normal_key_assignment() {
        let config = parse_replacing(
            "strategy: \"adaptive_hash_salt\"",
            "strategy: \"adaptive_hash_salt\"\n  normal_key_assignment: \"hash\"",
        )
        .expect("config with normal key assignment should parse");

        assert_eq!(
            config.partitioning.normal_key_assignment,
            NormalKeyAssignment::Hash
        );
    }

    #[test]
    fn parses_optional_resource_memory_guard() {
        let config = parse_replacing(
            "memory_limit_mb: 4096",
            "memory_limit_mb: 4096\n  fail_on_memory_limit: true",
        )
        .expect("config with memory guard should parse");

        assert!(config.resources.fail_on_memory_limit);
    }

    #[test]
    fn parses_optional_statistics_config() {
        let config = Config::from_yaml_str(&format!(
            "{EXAMPLE_CONFIG}\nstatistics:\n  heavy_hitter_mode: \"approximate\"\n  approximate_capacity: 512\n"
        ))
        .expect("config with statistics controls should parse");

        assert_eq!(
            config.statistics.heavy_hitter_mode,
            HeavyHitterMode::Approximate
        );
        assert_eq!(config.statistics.approximate_capacity.get(), 512);
    }

    #[test]
    fn parses_optional_storage_config() {
        let config = Config::from_yaml_str(&format!(
            "{EXAMPLE_CONFIG}\nstorage:\n  target_file_size_mb: 64\n  min_file_size_mb: 8\n"
        ))
        .expect("config with storage controls should parse");

        assert_eq!(config.storage.target_file_size_mb.get(), 64);
        assert_eq!(config.storage.min_file_size_mb.get(), 8);
    }

    #[test]
    fn parses_optional_output_config() {
        let config = Config::from_yaml_str(&format!(
            "{EXAMPLE_CONFIG}\noutput:\n  include_technical_columns: false\n  partition_column: \"partition_id\"\n  salt_column: \"salt\"\n  heavy_key_column: \"is_heavy\"\n"
        ))
        .expect("config with output controls should parse");

        assert!(!config.output.include_technical_columns);
        assert_eq!(
            config.output.path,
            PathBuf::from("./data/output_partitioned")
        );
        assert_eq!(config.output.format, DatasetFormat::Parquet);
        assert_eq!(config.output.partition_column, "partition_id");
        assert_eq!(config.output.salt_column, "salt");
        assert_eq!(config.output.heavy_key_column, "is_heavy");
    }

    #[test]
    fn parses_optional_join_config() {
        let config = Config::from_yaml_str(&format!(
            "{EXAMPLE_CONFIG}\njoin:\n  left_input: \"./data/left\"\n  right_input: \"./data/right\"\n  join_keys: [\"user_id\"]\n  right_side_mode: \"broadcast_if_small\"\n  broadcast_threshold_mb: 10\n"
        ))
        .expect("config with join controls should parse");

        let join = config.join.expect("join config should be present");
        assert_eq!(join.left_input, PathBuf::from("./data/left"));
        assert_eq!(join.right_input, PathBuf::from("./data/right"));
        assert_eq!(join.join_keys, vec!["user_id".to_string()]);
        assert_eq!(join.right_side_mode, RightSideMode::BroadcastIfSmall);
        assert_eq!(join.broadcast_threshold_mb, 10);
    }

    #[test]
    fn join_config_defaults_left_input_to_dataset_input() {
        let config = Config::from_yaml_str(&format!(
            "{EXAMPLE_CONFIG}\njoin:\n  right_input: \"./data/right\"\n  join_keys: [\"user_id\"]\n"
        ))
        .expect("config with minimal join controls should parse");

        let join = config.join.expect("join config should be present");
        assert_eq!(join.left_input, PathBuf::from("./data/input.parquet"));
        assert_eq!(join.right_side_mode, RightSideMode::BroadcastIfSmall);
        assert_eq!(join.broadcast_threshold_mb, 10);
    }

    #[test]
    fn rejects_missing_join_right_input() {
        let error = Config::from_yaml_str(&format!(
            "{EXAMPLE_CONFIG}\njoin:\n  right_input: \"\"\n  join_keys: [\"user_id\"]\n"
        ))
        .expect_err("empty join right input should be rejected");
        assert_validation_error(error, ConfigValidationError::MissingJoinRightInput);
    }

    #[test]
    fn rejects_missing_join_keys() {
        let error = Config::from_yaml_str(&format!(
            "{EXAMPLE_CONFIG}\njoin:\n  right_input: \"./data/right\"\n  join_keys: []\n"
        ))
        .expect_err("empty join keys should be rejected");
        assert_validation_error(error, ConfigValidationError::MissingJoinKeys);
    }

    #[test]
    fn rejects_invalid_min_partition_count() {
        let error = parse_replacing(
            "max_partitions: 128",
            "min_partitions: 0\n  max_partitions: 128",
        )
        .expect_err("zero min partitions should be rejected");
        assert_validation_error(
            error,
            ConfigValidationError::InvalidMinPartitionCount { value: 0 },
        );
    }

    #[test]
    fn rejects_min_partition_count_greater_than_max() {
        let error = parse_replacing(
            "max_partitions: 128",
            "min_partitions: 129\n  max_partitions: 128",
        )
        .expect_err("min partitions greater than max should be rejected");
        assert_validation_error(
            error,
            ConfigValidationError::MinPartitionsGreaterThanMax { min: 129, max: 128 },
        );
    }

    #[test]
    fn rejects_invalid_no_op_max_imbalance_ratio() {
        let error = parse_replacing(
            "heavy_key_alpha: 2.0",
            "heavy_key_alpha: 2.0\n  no_op_max_imbalance_ratio: 0.0",
        )
        .expect_err("zero no-op ratio should be rejected");
        assert_validation_error(
            error,
            ConfigValidationError::InvalidNoOpMaxImbalanceRatio { value: 0.0 },
        );
    }

    #[test]
    fn rejects_invalid_approximate_capacity() {
        let error = Config::from_yaml_str(&format!(
            "{EXAMPLE_CONFIG}\nstatistics:\n  heavy_hitter_mode: \"approximate\"\n  approximate_capacity: 0\n"
        ))
        .expect_err("zero approximate capacity should be rejected");
        assert_validation_error(
            error,
            ConfigValidationError::InvalidApproximateCapacity { value: 0 },
        );
    }

    #[test]
    fn rejects_invalid_storage_file_sizes() {
        let error = Config::from_yaml_str(&format!(
            "{EXAMPLE_CONFIG}\nstorage:\n  target_file_size_mb: 16\n  min_file_size_mb: 32\n"
        ))
        .expect_err("min file size greater than target should be rejected");
        assert_validation_error(
            error,
            ConfigValidationError::MinFileSizeGreaterThanTarget {
                min: 32,
                target: 16,
            },
        );
    }

    #[test]
    fn rejects_invalid_strategy_name() {
        let error = parse_replacing("strategy: \"adaptive_hash_salt\"", "strategy: \"range\"")
            .expect_err("bad strategy should be rejected");
        assert_validation_error(
            error,
            ConfigValidationError::InvalidStrategyName {
                value: "range".to_string(),
            },
        );
    }

    fn parse_replacing(target: &str, replacement: &str) -> Result<Config> {
        let config = EXAMPLE_CONFIG.replace(target, replacement);
        Config::from_yaml_str(&config)
    }

    fn assert_validation_error(error: Error, expected: ConfigValidationError) {
        match error {
            Error::InvalidConfig(actual) => assert_eq!(actual, expected),
            other => panic!("expected validation error, got {other:?}"),
        }
    }
}
