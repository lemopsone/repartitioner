use std::{fs, path::Path};

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Config {
    pub dataset: DatasetConfig,
    pub partitioning: PartitioningConfig,
    pub job: JobConfig,
    pub resources: ResourceConfig,
}

impl Config {
    pub fn from_yaml_str(input: &str) -> std::result::Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(input)
    }

    pub fn from_yaml_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path).map_err(|source| Error::ReadFile {
            path: path.to_path_buf(),
            source,
        })?;

        Self::from_yaml_str(&contents).map_err(|source| Error::ParseYaml {
            path: path.to_path_buf(),
            source,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetConfig {
    pub input: String,
    pub output: String,
    pub format: DatasetFormat,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatasetFormat {
    Parquet,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitioningConfig {
    pub key_columns: Vec<String>,
    pub target_partition_size_mb: u64,
    pub max_partitions: usize,
    pub strategy: PartitioningStrategy,
    pub heavy_key_alpha: f64,
    pub seed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitioningStrategy {
    AdaptiveHashSalt,
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

        assert_eq!(config.dataset.input, "./data/input.parquet");
        assert_eq!(config.dataset.format, DatasetFormat::Parquet);
        assert_eq!(config.partitioning.key_columns, vec!["user_id".to_string()]);
        assert_eq!(
            config.partitioning.strategy,
            PartitioningStrategy::AdaptiveHashSalt
        );
        assert_eq!(config.job.job_type, JobType::GroupBy);
        assert_eq!(config.job.downstream_engine, DownstreamEngine::Spark);
        assert_eq!(config.resources.local_threads, 8);
    }
}
