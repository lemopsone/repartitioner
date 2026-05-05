use std::{collections::BTreeMap, fs, path::Path};

use serde::{Deserialize, Serialize};

use crate::{config::PartitioningStrategy, Error, Result};

pub const METADATA_VERSION: &str = "0.1.0";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionPlan {
    pub version: String,
    pub created_at: String,
    pub strategy: PartitioningStrategy,
    pub key_columns: Vec<String>,
    pub min_partitions: usize,
    pub max_partitions: usize,
    pub target_partition_size_mb: u64,
    pub required_partitions_by_size: usize,
    pub target_partition_rows: u64,
    pub output_partitions: usize,
    pub feasibility: PartitionPlanFeasibility,
    pub normal_keys: Vec<NormalKeyPlan>,
    pub heavy_keys: Vec<HeavyKeyPlan>,
    pub hash_function: String,
    pub seed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionPlanFeasibility {
    pub target_partition_size_satisfied: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalKeyPlan {
    pub key: String,
    pub estimated_frequency: u64,
    pub partition_id: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeavyKeyPlan {
    pub key: String,
    pub estimated_frequency: u64,
    pub salt_count: usize,
    pub salt_partitions: Vec<SaltPartitionPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SaltPartitionPlan {
    pub salt_index: usize,
    pub partition_id: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatsMetadata {
    pub version: String,
    pub input: InputStats,
    pub skew: SkewStats,
    pub estimates: PartitionEstimates,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InputStats {
    pub total_rows: u64,
    pub input_files: Vec<InputFileStats>,
    pub estimated_row_width_bytes: Option<u64>,
    pub distinct_keys: Option<u64>,
    pub mean_key_frequency: f64,
    pub max_key_frequency: u64,
    pub key_frequencies: BTreeMap<String, u64>,
    pub heavy_hitter_candidates: Vec<HeavyKeyPlan>,
    pub heavy_hitters: Vec<HeavyKeyPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputFileStats {
    pub path: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SkewStats {
    pub max_partition_size: u64,
    pub mean_partition_size: f64,
    pub median_partition_size: f64,
    pub p95_partition_size: f64,
    pub partition_size_variance: f64,
    pub coefficient_of_variation: f64,
    pub max_mean_imbalance_ratio: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionEstimates {
    pub target_partitions: usize,
    pub before_partition_sizes: Vec<u64>,
    pub after_partition_sizes: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub version: String,
    pub output_files: Vec<OutputFile>,
    pub partitions: Vec<PartitionManifest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputFile {
    pub path: String,
    pub partition_id: usize,
    pub row_count: u64,
    pub size_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionManifest {
    pub partition_id: usize,
    pub row_count: u64,
    pub file_count: usize,
    pub size_bytes: Option<u64>,
}

pub fn write_json_metadata<T>(path: impl AsRef<Path>, value: &T) -> Result<()>
where
    T: Serialize,
{
    let path = path.as_ref();
    let payload = serde_json::to_vec_pretty(value)?;
    fs::write(path, payload).map_err(|source| Error::WriteFile {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PartitioningStrategy;

    #[test]
    fn serializes_partition_plan_metadata() {
        let plan = PartitionPlan {
            version: METADATA_VERSION.to_string(),
            created_at: "2026-05-02T00:00:00Z".to_string(),
            strategy: PartitioningStrategy::AdaptiveHashSalt,
            key_columns: vec!["user_id".to_string()],
            min_partitions: 1,
            max_partitions: 4,
            target_partition_size_mb: 128,
            required_partitions_by_size: 4,
            target_partition_rows: 250,
            output_partitions: 4,
            feasibility: PartitionPlanFeasibility {
                target_partition_size_satisfied: true,
                reason: None,
            },
            normal_keys: vec![NormalKeyPlan {
                key: "user_id=7".to_string(),
                estimated_frequency: 10,
                partition_id: 2,
            }],
            heavy_keys: vec![HeavyKeyPlan {
                key: "42".to_string(),
                estimated_frequency: 1000,
                salt_count: 3,
                salt_partitions: vec![
                    SaltPartitionPlan {
                        salt_index: 0,
                        partition_id: 0,
                    },
                    SaltPartitionPlan {
                        salt_index: 1,
                        partition_id: 2,
                    },
                    SaltPartitionPlan {
                        salt_index: 2,
                        partition_id: 3,
                    },
                ],
            }],
            hash_function: "fnv1a64_seeded".to_string(),
            seed: 42,
        };

        let json = serde_json::to_string(&plan).expect("plan should serialize");
        assert!(json.contains("\"strategy\":\"adaptive_hash_salt\""));
        assert!(json.contains("\"normal_keys\""));
        assert!(json.contains("\"salt_count\":3"));
        assert!(json.contains("\"salt_partitions\""));
    }

    #[test]
    fn serializes_stats_and_manifest_metadata() {
        let stats = StatsMetadata {
            version: METADATA_VERSION.to_string(),
            input: InputStats {
                total_rows: 10,
                input_files: vec![InputFileStats {
                    path: "input.parquet".to_string(),
                    size_bytes: 2048,
                }],
                estimated_row_width_bytes: Some(128),
                distinct_keys: Some(2),
                mean_key_frequency: 5.0,
                max_key_frequency: 5,
                key_frequencies: BTreeMap::from([("a".to_string(), 5), ("b".to_string(), 5)]),
                heavy_hitter_candidates: Vec::new(),
                heavy_hitters: Vec::new(),
            },
            skew: SkewStats {
                max_partition_size: 5,
                mean_partition_size: 5.0,
                median_partition_size: 5.0,
                p95_partition_size: 5.0,
                partition_size_variance: 0.0,
                coefficient_of_variation: 0.0,
                max_mean_imbalance_ratio: 1.0,
            },
            estimates: PartitionEstimates {
                target_partitions: 2,
                before_partition_sizes: vec![5, 5],
                after_partition_sizes: vec![5, 5],
            },
        };

        let manifest = Manifest {
            version: METADATA_VERSION.to_string(),
            output_files: vec![OutputFile {
                path: "ap_partition=0/part-00000.parquet".to_string(),
                partition_id: 0,
                row_count: 5,
                size_bytes: Some(1024),
            }],
            partitions: vec![PartitionManifest {
                partition_id: 0,
                row_count: 5,
                file_count: 1,
                size_bytes: Some(1024),
            }],
        };

        let stats_json = serde_json::to_string(&stats).expect("stats should serialize");
        let manifest_json = serde_json::to_string(&manifest).expect("manifest should serialize");

        assert!(stats_json.contains("\"total_rows\":10"));
        assert!(stats_json.contains("\"coefficient_of_variation\":0.0"));
        assert!(manifest_json.contains("\"ap_partition=0/part-00000.parquet\""));
    }
}
