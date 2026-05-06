pub mod cli;
pub mod config;
pub mod dataset;
pub mod error;
pub mod hashing;
pub mod heavy_hitters;
pub mod key_encoding;
pub mod manifest;
pub mod partitioner;
pub mod planner;
pub mod reader;
pub mod statistics;
pub mod targeting;
pub mod writer;

pub use config::Config;
pub use error::{Error, Result};

#[cfg(test)]
pub(crate) mod tests {
    use crate::Config;

    pub(crate) fn example_config() -> Config {
        Config::from_yaml_str(
            r#"
dataset:
  input: "./data/input.parquet"
  output: "./data/output_partitioned"
  format: "parquet"

partitioning:
  key_columns: ["user_id"]
  target_partition_size_mb: 128
  max_partitions: 4
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
        .expect("example config should be valid")
    }
}
