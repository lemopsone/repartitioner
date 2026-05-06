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
    pub output: OutputConfig,
    pub job: JobConfig,
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
    pub output: PathBuf,
    pub format: DatasetFormat,
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
    pub heavy_key_alpha: f64,
    pub force_rewrite: bool,
    pub no_op_max_imbalance_ratio: f64,
    pub seed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputConfig {
    pub include_technical_columns: bool,
    pub partition_column: String,
    pub salt_column: String,
    pub heavy_key_column: String,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
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
}

impl FromStr for PartitioningStrategy {
    type Err = ConfigValidationError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "adaptive_hash_salt" => Ok(Self::AdaptiveHashSalt),
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
    output: Option<RawOutputConfig>,
    job: JobConfig,
    resources: RawResourceConfig,
}

#[derive(Debug, Deserialize)]
struct RawDatasetConfig {
    input: String,
    output: String,
    format: DatasetFormat,
}

#[derive(Debug, Deserialize)]
struct RawPartitioningConfig {
    key_columns: Vec<String>,
    min_partitions: Option<usize>,
    target_partition_size_mb: u64,
    max_partitions: usize,
    strategy: String,
    heavy_key_alpha: f64,
    force_rewrite: Option<bool>,
    no_op_max_imbalance_ratio: Option<f64>,
    seed: u64,
}

#[derive(Debug, Deserialize)]
struct RawOutputConfig {
    include_technical_columns: Option<bool>,
    partition_column: Option<String>,
    salt_column: Option<String>,
    heavy_key_column: Option<String>,
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
        let output = output_config(raw.output);

        let config = Config {
            dataset: DatasetConfig {
                input: PathBuf::from(raw.dataset.input),
                output: PathBuf::from(raw.dataset.output),
                format: raw.dataset.format,
            },
            partitioning: PartitioningConfig {
                key_columns: raw.partitioning.key_columns,
                min_partitions,
                target_partition_size_mb,
                max_partitions,
                strategy,
                heavy_key_alpha: raw.partitioning.heavy_key_alpha,
                force_rewrite: raw.partitioning.force_rewrite.unwrap_or(false),
                no_op_max_imbalance_ratio,
                seed: raw.partitioning.seed,
            },
            output,
            job: raw.job,
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

fn output_config(raw: Option<RawOutputConfig>) -> OutputConfig {
    let defaults = OutputConfig::default();
    match raw {
        Some(raw) => OutputConfig {
            include_technical_columns: raw
                .include_technical_columns
                .unwrap_or(defaults.include_technical_columns),
            partition_column: raw.partition_column.unwrap_or(defaults.partition_column),
            salt_column: raw.salt_column.unwrap_or(defaults.salt_column),
            heavy_key_column: raw.heavy_key_column.unwrap_or(defaults.heavy_key_column),
        },
        None => defaults,
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
        assert_eq!(config.dataset.format, DatasetFormat::Parquet);
        assert_eq!(config.partitioning.key_columns, vec!["user_id".to_string()]);
        assert_eq!(config.partitioning.min_partitions.get(), 1);
        assert_eq!(config.partitioning.target_partition_size_mb.get(), 128);
        assert_eq!(config.partitioning.max_partitions.get(), 128);
        assert!(!config.partitioning.force_rewrite);
        assert_eq!(config.partitioning.no_op_max_imbalance_ratio, 1.2);
        assert!(config.output.include_technical_columns);
        assert_eq!(config.output.partition_column, "_rp_partition_id");
        assert_eq!(config.output.salt_column, "_rp_salt");
        assert_eq!(config.output.heavy_key_column, "_rp_is_heavy_key");
        assert_eq!(
            config.partitioning.strategy,
            PartitioningStrategy::AdaptiveHashSalt
        );
        assert_eq!(config.job.job_type, JobType::GroupBy);
        assert_eq!(config.job.downstream_engine, DownstreamEngine::Spark);
        assert_eq!(config.resources.local_threads, 8);
        assert_eq!(config.resources.memory_limit_mb, 4096);
        assert!(!config.resources.fail_on_memory_limit);
    }

    #[test]
    fn parses_csv_dataset_format() {
        let config = parse_replacing("format: \"parquet\"", "format: \"csv\"")
            .expect("csv config should parse");

        assert_eq!(config.dataset.format, DatasetFormat::Csv);
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
    fn parses_optional_resource_memory_guard() {
        let config = parse_replacing(
            "memory_limit_mb: 4096",
            "memory_limit_mb: 4096\n  fail_on_memory_limit: true",
        )
        .expect("config with memory guard should parse");

        assert!(config.resources.fail_on_memory_limit);
    }

    #[test]
    fn parses_optional_output_config() {
        let config = Config::from_yaml_str(&format!(
            "{EXAMPLE_CONFIG}\noutput:\n  include_technical_columns: false\n  partition_column: \"partition_id\"\n  salt_column: \"salt\"\n  heavy_key_column: \"is_heavy\"\n"
        ))
        .expect("config with output controls should parse");

        assert!(!config.output.include_technical_columns);
        assert_eq!(config.output.partition_column, "partition_id");
        assert_eq!(config.output.salt_column, "salt");
        assert_eq!(config.output.heavy_key_column, "is_heavy");
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
