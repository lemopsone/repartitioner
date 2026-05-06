use std::{collections::BTreeMap, fs, path::Path};

use serde::{Deserialize, Serialize};

use crate::{
    config::{DownstreamEngine, JobType, PartitioningStrategy},
    Error, Result,
};

pub const METADATA_VERSION: &str = "0.2.0";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionPlan {
    pub version: String,
    pub created_at: String,
    pub strategy: PartitioningStrategy,
    pub action: PlanAction,
    pub rewrite_required: bool,
    pub key_columns: Vec<String>,
    pub job_type: JobType,
    pub downstream_engine: DownstreamEngine,
    pub target_partition_size_mb: u64,
    pub target_partition_rows: u64,
    pub min_partitions: usize,
    pub max_partitions: usize,
    pub output_partitions: usize,
    pub required_partitions_by_size: usize,
    pub feasibility: PartitionPlanFeasibility,
    pub technical_columns: TechnicalColumns,
    pub normal_keys: Vec<NormalKeyPlan>,
    pub heavy_keys: Vec<HeavyKeyPlan>,
    pub recommended_downstream_plan: RecommendedDownstreamPlan,
    pub join_plan: Option<JoinPlan>,
    pub cost_estimate: CostEstimate,
    pub skip_reason: Option<String>,
    pub hash_function: String,
    pub seed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinPlan {
    pub left_heavy_keys: Vec<String>,
    pub right_heavy_keys: Vec<String>,
    pub shared_heavy_keys: Vec<String>,
    pub recommended_strategy: String,
    pub right_side_size_mb: Option<u64>,
    pub broadcast_threshold_mb: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionPlanFeasibility {
    pub target_partition_size_satisfied: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanAction {
    NoOp,
    Rewrite,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CostEstimate {
    pub estimated_rows_read: u64,
    pub estimated_rows_written: u64,
    pub estimated_bytes_read: Option<u64>,
    pub estimated_bytes_written: Option<u64>,
    pub rewrite_required: bool,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TechnicalColumns {
    pub included: bool,
    pub partition_column: String,
    pub salt_column: String,
    pub heavy_key_column: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecommendedDownstreamPlan {
    pub job_type: JobType,
    pub strategy: String,
    pub requires_operator_rewrite: bool,
    pub partition_column: Option<String>,
    pub salt_column: Option<String>,
    pub heavy_key_column: Option<String>,
    pub partial_group_keys: Vec<String>,
    pub final_group_keys: Vec<String>,
    pub join_keys: Vec<String>,
    pub notes: Vec<String>,
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
    pub detection_reasons: Vec<HeavyKeyReason>,
    pub salt_count: usize,
    pub salt_partitions: Vec<SaltPartitionPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HeavyKeyReason {
    AboveMeanThreshold,
    ExceedsTargetPartitionRows,
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
    pub heavy_hitter_detection: HeavyHitterDetectionMetadata,
    pub storage: StorageMetadata,
    pub join: Option<JoinStatisticsMetadata>,
    pub skew: SkewStats,
    pub estimates: PartitionEstimates,
    pub resources: ResourceEstimate,
    pub timing: Option<TimingMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinStatisticsMetadata {
    pub join_keys: Vec<String>,
    pub left: JoinSideStatistics,
    pub right: Option<JoinSideStatistics>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinSideStatistics {
    pub total_rows: u64,
    pub total_size_bytes: Option<u64>,
    pub estimated_size_mb: Option<u64>,
    pub heavy_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeavyHitterDetectionMetadata {
    pub mode: String,
    pub capacity: usize,
    pub error_bound: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimingMetadata {
    pub read_seconds: f64,
    pub statistics_seconds: f64,
    pub planning_seconds: f64,
    pub assignment_seconds: f64,
    pub writing_seconds: f64,
    pub total_seconds: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceEstimate {
    pub configured_memory_limit_mb: u64,
    pub estimated_dataset_size_mb: Option<u64>,
    pub in_memory_processing_used: bool,
    pub memory_limit_exceeded: bool,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InputStats {
    pub total_rows: u64,
    pub input_file_count: usize,
    pub input_files: Vec<InputFileStats>,
    pub min_file_size_bytes: Option<u64>,
    pub max_file_size_bytes: Option<u64>,
    pub mean_file_size_bytes: Option<f64>,
    pub small_file_count: usize,
    pub oversized_file_count: usize,
    pub estimated_row_width_bytes: Option<u64>,
    pub distinct_keys: Option<u64>,
    pub mean_key_frequency: f64,
    pub max_key_frequency: u64,
    pub key_frequencies: BTreeMap<String, u64>,
    pub heavy_hitter_candidates: Vec<HeavyKeyPlan>,
    pub heavy_hitters: Vec<HeavyKeyPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageMetadata {
    pub target_file_size_mb: u64,
    pub min_file_size_mb: u64,
    pub target_file_size_bytes: u64,
    pub min_file_size_bytes: u64,
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
    pub input_reused: bool,
    pub dataset_location: Option<String>,
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
            action: PlanAction::Rewrite,
            rewrite_required: true,
            key_columns: vec!["user_id".to_string()],
            job_type: JobType::GroupBy,
            downstream_engine: DownstreamEngine::Spark,
            target_partition_size_mb: 128,
            target_partition_rows: 250,
            min_partitions: 1,
            max_partitions: 4,
            output_partitions: 4,
            required_partitions_by_size: 4,
            feasibility: PartitionPlanFeasibility {
                target_partition_size_satisfied: true,
                reason: None,
            },
            technical_columns: TechnicalColumns {
                included: true,
                partition_column: "_rp_partition_id".to_string(),
                salt_column: "_rp_salt".to_string(),
                heavy_key_column: "_rp_is_heavy_key".to_string(),
            },
            normal_keys: vec![NormalKeyPlan {
                key: "user_id=7".to_string(),
                estimated_frequency: 10,
                partition_id: 2,
            }],
            heavy_keys: vec![HeavyKeyPlan {
                key: "42".to_string(),
                estimated_frequency: 1000,
                detection_reasons: vec![HeavyKeyReason::AboveMeanThreshold],
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
            recommended_downstream_plan: RecommendedDownstreamPlan {
                job_type: JobType::GroupBy,
                strategy: "two_stage_group_by".to_string(),
                requires_operator_rewrite: true,
                partition_column: Some("_rp_partition_id".to_string()),
                salt_column: Some("_rp_salt".to_string()),
                heavy_key_column: Some("_rp_is_heavy_key".to_string()),
                partial_group_keys: vec!["_rp_partition_id".to_string(), "user_id".to_string()],
                final_group_keys: vec!["user_id".to_string()],
                join_keys: Vec::new(),
                notes: Vec::new(),
            },
            join_plan: None,
            cost_estimate: CostEstimate {
                estimated_rows_read: 10,
                estimated_rows_written: 10,
                estimated_bytes_read: Some(2048),
                estimated_bytes_written: Some(2048),
                rewrite_required: true,
                reason: "heavy_keys_detected".to_string(),
            },
            skip_reason: None,
            hash_function: "fnv1a64_seeded".to_string(),
            seed: 42,
        };

        let json = serde_json::to_string(&plan).expect("plan should serialize");
        let value: serde_json::Value = serde_json::from_str(&json).expect("plan should parse");

        assert_eq!(value["version"].as_str(), Some("0.2.0"));
        assert_eq!(value["action"].as_str(), Some("rewrite"));
        assert_eq!(value["rewrite_required"].as_bool(), Some(true));
        assert_eq!(value["hash_function"].as_str(), Some("fnv1a64_seeded"));
        assert!(value["cost_estimate"].is_object());
        assert!(json.contains("\"strategy\":\"adaptive_hash_salt\""));
        assert!(json.contains("\"normal_keys\""));
        assert!(json.contains("\"salt_count\":3"));
        assert!(json.contains("\"salt_partitions\""));
        assert!(json.contains("\"job_type\":\"group_by\""));
        assert!(json.contains("\"recommended_downstream_plan\""));
        assert!(json.contains("\"two_stage_group_by\""));
    }

    #[test]
    fn serializes_stats_and_manifest_metadata() {
        let stats = StatsMetadata {
            version: METADATA_VERSION.to_string(),
            input: InputStats {
                total_rows: 10,
                input_file_count: 1,
                input_files: vec![InputFileStats {
                    path: "input.parquet".to_string(),
                    size_bytes: 2048,
                }],
                min_file_size_bytes: Some(2048),
                max_file_size_bytes: Some(2048),
                mean_file_size_bytes: Some(2048.0),
                small_file_count: 1,
                oversized_file_count: 0,
                estimated_row_width_bytes: Some(128),
                distinct_keys: Some(2),
                mean_key_frequency: 5.0,
                max_key_frequency: 5,
                key_frequencies: BTreeMap::from([("a".to_string(), 5), ("b".to_string(), 5)]),
                heavy_hitter_candidates: Vec::new(),
                heavy_hitters: Vec::new(),
            },
            heavy_hitter_detection: HeavyHitterDetectionMetadata {
                mode: "exact".to_string(),
                capacity: 10_000,
                error_bound: "0".to_string(),
            },
            storage: StorageMetadata {
                target_file_size_mb: 128,
                min_file_size_mb: 16,
                target_file_size_bytes: 134_217_728,
                min_file_size_bytes: 16_777_216,
            },
            join: None,
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
            resources: ResourceEstimate {
                configured_memory_limit_mb: 4096,
                estimated_dataset_size_mb: Some(1),
                in_memory_processing_used: true,
                memory_limit_exceeded: false,
                warnings: Vec::new(),
            },
            timing: Some(TimingMetadata {
                read_seconds: 0.1,
                statistics_seconds: 0.2,
                planning_seconds: 0.3,
                assignment_seconds: 0.4,
                writing_seconds: 0.5,
                total_seconds: 1.5,
            }),
        };

        let manifest = Manifest {
            version: METADATA_VERSION.to_string(),
            input_reused: false,
            dataset_location: Some("output_dataset".to_string()),
            output_files: vec![OutputFile {
                path: "rp_partition=0/part-00000.parquet".to_string(),
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
        let stats_value: serde_json::Value =
            serde_json::from_str(&stats_json).expect("stats should parse");
        let manifest_value: serde_json::Value =
            serde_json::from_str(&manifest_json).expect("manifest should parse");

        assert_eq!(stats_value["version"].as_str(), Some("0.2.0"));
        assert!(stats_value["input"].is_object());
        assert_eq!(
            stats_value["heavy_hitter_detection"]["mode"].as_str(),
            Some("exact")
        );
        assert_eq!(stats_value["input"]["input_file_count"].as_u64(), Some(1));
        assert_eq!(stats_value["input"]["small_file_count"].as_u64(), Some(1));
        assert!(stats_value["storage"].is_object());
        assert!(stats_value["skew"].is_object());
        assert!(stats_value["estimates"].is_object());
        assert!(stats_value["resources"].is_object());
        assert_eq!(stats_value["timing"]["read_seconds"].as_f64(), Some(0.1));
        assert_eq!(stats_value["timing"]["total_seconds"].as_f64(), Some(1.5));
        assert!(stats_json.contains("\"total_rows\":10"));
        assert!(stats_json.contains("\"coefficient_of_variation\":0.0"));
        assert_eq!(manifest_value["version"].as_str(), Some("0.2.0"));
        assert_eq!(
            manifest_value["dataset_location"].as_str(),
            Some("output_dataset")
        );
        assert!(manifest_json.contains("\"rp_partition=0/part-00000.parquet\""));
    }
}
