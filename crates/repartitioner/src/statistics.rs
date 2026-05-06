use std::collections::BTreeMap;

use crate::{
    config::HeavyHitterMode,
    hashing, heavy_hitters,
    manifest::{
        HeavyHitterDetectionMetadata, HeavyKeyPlan, InputFileStats, InputStats, PartitionEstimates,
        ResourceEstimate, SkewStats, StatsMetadata, StorageMetadata, TimingMetadata,
        METADATA_VERSION,
    },
    reader::InputDataset,
    targeting, Config, Error, Result,
};

#[derive(Debug, Clone, PartialEq)]
pub struct ComputedStatistics {
    pub metadata: StatsMetadata,
}

impl ComputedStatistics {
    pub fn set_after_partition_sizes(&mut self, partition_sizes: Vec<u64>) {
        self.metadata.estimates.after_partition_sizes = partition_sizes;
    }

    pub fn set_timing(&mut self, timing: TimingMetadata) {
        self.metadata.timing = Some(timing);
    }
}

pub fn compute_statistics(config: &Config, dataset: &InputDataset) -> Result<ComputedStatistics> {
    let resources = resource_estimate(config, dataset)?;
    let file_size_summary = file_size_summary(config, dataset);
    let key_frequency_summary = key_frequency_summary(dataset, config);
    let key_frequencies = key_frequency_summary.frequencies;
    let mean_key_frequency = mean_frequency(&key_frequencies);
    let max_key_frequency = max_frequency(&key_frequencies);
    let estimated_row_width_bytes = estimated_row_width_bytes(dataset);
    let target_partitioning = targeting::compute_target_partitioning(
        config,
        dataset.rows.row_count(),
        estimated_row_width_bytes,
    );
    let heavy_hitter_candidates = heavy_hitters::detect_heavy_hitter_candidates(
        &key_frequencies,
        config.partitioning.heavy_key_alpha,
    )
    .into_iter()
    .map(heavy_key_plan_placeholder)
    .collect();
    let heavy_hitters = heavy_hitters::detect_final_heavy_keys(
        &key_frequencies,
        config.partitioning.heavy_key_alpha,
        target_partitioning.target_partition_rows,
    )
    .into_iter()
    .map(heavy_key_plan_placeholder)
    .collect();
    let partition_sizes = base_partition_sizes(
        dataset,
        &config.partitioning.key_columns,
        target_partitioning.output_partitions,
        config.partitioning.seed,
    );
    let skew = skew_stats(&partition_sizes);

    let metadata = StatsMetadata {
        version: METADATA_VERSION.to_string(),
        input: InputStats {
            total_rows: dataset.rows.row_count(),
            input_file_count: dataset.files.len(),
            input_files: dataset
                .files
                .iter()
                .map(|file| InputFileStats {
                    path: file.path.clone(),
                    size_bytes: file.size_bytes,
                })
                .collect(),
            min_file_size_bytes: file_size_summary.min_file_size_bytes,
            max_file_size_bytes: file_size_summary.max_file_size_bytes,
            mean_file_size_bytes: file_size_summary.mean_file_size_bytes,
            small_file_count: file_size_summary.small_file_count,
            oversized_file_count: file_size_summary.oversized_file_count,
            estimated_row_width_bytes,
            distinct_keys: Some(key_frequencies.len() as u64),
            mean_key_frequency,
            max_key_frequency,
            key_frequencies,
            heavy_hitter_candidates,
            heavy_hitters,
        },
        heavy_hitter_detection: key_frequency_summary.detection_metadata,
        storage: StorageMetadata {
            target_file_size_mb: config.storage.target_file_size_mb.get(),
            min_file_size_mb: config.storage.min_file_size_mb.get(),
            target_file_size_bytes: mb_to_bytes(config.storage.target_file_size_mb.get()),
            min_file_size_bytes: mb_to_bytes(config.storage.min_file_size_mb.get()),
        },
        skew,
        estimates: PartitionEstimates {
            target_partitions: target_partitioning.output_partitions,
            before_partition_sizes: partition_sizes,
            after_partition_sizes: vec![0; target_partitioning.output_partitions],
        },
        resources,
        timing: None,
    };

    Ok(ComputedStatistics { metadata })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KeyFrequencySummary {
    frequencies: BTreeMap<String, u64>,
    detection_metadata: HeavyHitterDetectionMetadata,
}

#[derive(Debug, Clone, PartialEq)]
struct FileSizeSummary {
    min_file_size_bytes: Option<u64>,
    max_file_size_bytes: Option<u64>,
    mean_file_size_bytes: Option<f64>,
    small_file_count: usize,
    oversized_file_count: usize,
}

fn file_size_summary(config: &Config, dataset: &InputDataset) -> FileSizeSummary {
    let file_sizes = dataset
        .files
        .iter()
        .map(|file| file.size_bytes)
        .collect::<Vec<_>>();
    if file_sizes.is_empty() {
        return FileSizeSummary {
            min_file_size_bytes: None,
            max_file_size_bytes: None,
            mean_file_size_bytes: None,
            small_file_count: 0,
            oversized_file_count: 0,
        };
    }

    let min_file_size_bytes = file_sizes.iter().copied().min();
    let max_file_size_bytes = file_sizes.iter().copied().max();
    let mean_file_size_bytes =
        Some(file_sizes.iter().sum::<u64>() as f64 / file_sizes.len() as f64);
    let min_size_bytes = mb_to_bytes(config.storage.min_file_size_mb.get());
    let target_size_bytes = mb_to_bytes(config.storage.target_file_size_mb.get());

    FileSizeSummary {
        min_file_size_bytes,
        max_file_size_bytes,
        mean_file_size_bytes,
        small_file_count: file_sizes
            .iter()
            .filter(|size_bytes| **size_bytes < min_size_bytes)
            .count(),
        oversized_file_count: file_sizes
            .iter()
            .filter(|size_bytes| **size_bytes > target_size_bytes)
            .count(),
    }
}

fn mb_to_bytes(value: u64) -> u64 {
    value.saturating_mul(1024 * 1024)
}

fn resource_estimate(config: &Config, dataset: &InputDataset) -> Result<ResourceEstimate> {
    let estimated_dataset_size_mb = estimated_dataset_size_mb(dataset);
    let memory_limit_exceeded =
        estimated_dataset_size_mb.is_some_and(|size_mb| size_mb > config.resources.memory_limit_mb);

    if memory_limit_exceeded && config.resources.fail_on_memory_limit {
        return Err(Error::ResourceLimitExceeded {
            configured_memory_limit_mb: config.resources.memory_limit_mb,
            estimated_dataset_size_mb: estimated_dataset_size_mb.unwrap_or_default(),
        });
    }

    let mut warnings = Vec::new();
    if memory_limit_exceeded {
        warnings.push("estimated_dataset_size_exceeds_configured_memory_limit".to_string());
    }

    Ok(ResourceEstimate {
        configured_memory_limit_mb: config.resources.memory_limit_mb,
        estimated_dataset_size_mb,
        in_memory_processing_used: true,
        memory_limit_exceeded,
        warnings,
    })
}

fn estimated_dataset_size_mb(dataset: &InputDataset) -> Option<u64> {
    let total_size_bytes = dataset
        .files
        .iter()
        .map(|file| file.size_bytes)
        .sum::<u64>();

    (total_size_bytes > 0).then_some(total_size_bytes.div_ceil(1024 * 1024))
}

fn heavy_key_plan_placeholder(heavy: heavy_hitters::HeavyHitter) -> HeavyKeyPlan {
    HeavyKeyPlan {
        key: heavy.key,
        estimated_frequency: heavy.frequency,
        detection_reasons: heavy.detection_reasons,
        salt_count: 1,
        salt_partitions: Vec::new(),
    }
}

fn estimated_row_width_bytes(dataset: &InputDataset) -> Option<u64> {
    let total_rows = dataset.rows.row_count();
    if total_rows == 0 {
        return None;
    }

    let total_size_bytes = dataset
        .files
        .iter()
        .map(|file| file.size_bytes)
        .sum::<u64>();
    if total_size_bytes == 0 {
        return None;
    }

    Some(total_size_bytes.div_ceil(total_rows).max(1))
}

fn key_frequency_summary(dataset: &InputDataset, config: &Config) -> KeyFrequencySummary {
    match config.statistics.heavy_hitter_mode {
        HeavyHitterMode::Exact => KeyFrequencySummary {
            frequencies: key_frequencies(dataset, &config.partitioning.key_columns),
            detection_metadata: HeavyHitterDetectionMetadata {
                mode: "exact".to_string(),
                capacity: config.statistics.approximate_capacity.get(),
                error_bound: "0".to_string(),
            },
        },
        HeavyHitterMode::Approximate => {
            let summary = heavy_hitters::space_saving(
                dataset
                    .rows
                    .rows
                    .iter()
                    .filter_map(|row| row.partition_key(&config.partitioning.key_columns)),
                config.statistics.approximate_capacity.get(),
            );
            KeyFrequencySummary {
                frequencies: summary.frequencies,
                detection_metadata: HeavyHitterDetectionMetadata {
                    mode: "approximate".to_string(),
                    capacity: config.statistics.approximate_capacity.get(),
                    error_bound: format!("space_saving_max_overestimate={}", summary.max_error),
                },
            }
        }
    }
}

fn key_frequencies(dataset: &InputDataset, key_columns: &[String]) -> BTreeMap<String, u64> {
    let mut frequencies = BTreeMap::new();

    for row in &dataset.rows.rows {
        if let Some(key) = row.partition_key(key_columns) {
            *frequencies.entry(key).or_insert(0) += 1;
        }
    }

    frequencies
}

fn mean_frequency(key_frequencies: &BTreeMap<String, u64>) -> f64 {
    if key_frequencies.is_empty() {
        return 0.0;
    }

    key_frequencies.values().sum::<u64>() as f64 / key_frequencies.len() as f64
}

fn max_frequency(key_frequencies: &BTreeMap<String, u64>) -> u64 {
    key_frequencies.values().copied().max().unwrap_or(0)
}

fn base_partition_sizes(
    dataset: &InputDataset,
    key_columns: &[String],
    partition_count: usize,
    seed: u64,
) -> Vec<u64> {
    let mut sizes = vec![0; partition_count];

    if partition_count == 0 {
        return sizes;
    }

    for row in &dataset.rows.rows {
        if let Some(key) = row.partition_key(key_columns) {
            let partition_id = hashing::partition_id(&key, partition_count, seed);
            sizes[partition_id] += 1;
        }
    }

    sizes
}

fn skew_stats(partition_sizes: &[u64]) -> SkewStats {
    if partition_sizes.is_empty() {
        return SkewStats {
            max_partition_size: 0,
            mean_partition_size: 0.0,
            median_partition_size: 0.0,
            p95_partition_size: 0.0,
            partition_size_variance: 0.0,
            coefficient_of_variation: 0.0,
            max_mean_imbalance_ratio: 0.0,
        };
    }

    let mut sorted = partition_sizes.to_vec();
    sorted.sort_unstable();

    let max_partition_size = *sorted.last().unwrap_or(&0);
    let mean_partition_size =
        partition_sizes.iter().sum::<u64>() as f64 / partition_sizes.len() as f64;
    let median_partition_size = percentile(&sorted, 0.50);
    let p95_partition_size = percentile(&sorted, 0.95);
    let partition_size_variance = partition_sizes
        .iter()
        .map(|size| {
            let difference = *size as f64 - mean_partition_size;
            difference * difference
        })
        .sum::<f64>()
        / partition_sizes.len() as f64;
    let coefficient_of_variation = if mean_partition_size > 0.0 {
        partition_size_variance.sqrt() / mean_partition_size
    } else {
        0.0
    };
    let max_mean_imbalance_ratio = if mean_partition_size > 0.0 {
        max_partition_size as f64 / mean_partition_size
    } else {
        0.0
    };

    SkewStats {
        max_partition_size,
        mean_partition_size,
        median_partition_size,
        p95_partition_size,
        partition_size_variance,
        coefficient_of_variation,
        max_mean_imbalance_ratio,
    }
}

fn percentile(sorted_values: &[u64], percentile: f64) -> f64 {
    if sorted_values.is_empty() {
        return 0.0;
    }

    let index = ((sorted_values.len() - 1) as f64 * percentile).ceil() as usize;
    sorted_values[index] as f64
}

#[cfg(test)]
mod tests {
    use crate::{
        config::{Config, DatasetFormat},
        dataset::Dataset,
        reader::{InputDataset, InputFile},
        tests::example_config,
    };

    use super::*;

    #[test]
    fn computes_key_statistics_from_in_memory_rows() {
        let config = example_config();
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            ["a", "a", "b", "c"].into_iter().map(String::from),
        ));

        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");

        assert_eq!(statistics.metadata.input.total_rows, 4);
        assert_eq!(statistics.metadata.input.distinct_keys, Some(3));
        assert_eq!(statistics.metadata.input.mean_key_frequency, 4.0 / 3.0);
        assert_eq!(statistics.metadata.input.max_key_frequency, 2);
        assert_eq!(
            statistics
                .metadata
                .input
                .key_frequencies
                .get("7:user_id#utf8:1:a"),
            Some(&2)
        );
    }

    #[test]
    fn detects_heavy_key_from_in_memory_rows() {
        let config = example_config();
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            ["heavy", "heavy", "heavy", "heavy", "heavy", "a", "b"]
                .into_iter()
                .map(String::from),
        ));

        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");

        assert_eq!(statistics.metadata.input.heavy_hitters.len(), 1);
        assert_eq!(statistics.metadata.input.heavy_hitter_candidates.len(), 1);
        assert_eq!(
            statistics.metadata.input.heavy_hitters[0].key,
            "7:user_id#utf8:5:heavy"
        );
        assert_eq!(
            statistics.metadata.input.heavy_hitters[0].estimated_frequency,
            5
        );
    }

    #[test]
    fn approximate_heavy_hitter_mode_detects_synthetic_heavy_key() {
        let config = Config::from_yaml_str(
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

statistics:
  heavy_hitter_mode: "approximate"
  approximate_capacity: 32

job:
  type: "group_by"
  downstream_engine: "spark"

resources:
  local_threads: 8
  memory_limit_mb: 4096
"#,
        )
        .expect("config should parse");
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            std::iter::repeat_n("heavy".to_string(), 200)
                .chain((0..800).map(|index| format!("key_{index}"))),
        ));

        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");

        assert_eq!(
            statistics.metadata.heavy_hitter_detection.mode,
            "approximate"
        );
        assert_eq!(statistics.metadata.heavy_hitter_detection.capacity, 32);
        assert!(statistics
            .metadata
            .heavy_hitter_detection
            .error_bound
            .starts_with("space_saving_max_overestimate="));
        assert!(statistics
            .metadata
            .input
            .heavy_hitters
            .iter()
            .any(|heavy| heavy.key == "7:user_id#utf8:5:heavy"));
        assert!(statistics.metadata.input.key_frequencies.len() <= 32);
    }

    #[test]
    fn does_not_overreact_to_uniform_in_memory_rows() {
        let config = example_config();
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            ["a", "a", "b", "b", "c", "c"].into_iter().map(String::from),
        ));

        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");

        assert!(statistics.metadata.input.heavy_hitters.is_empty());
        assert!(statistics.metadata.input.heavy_hitter_candidates.is_empty());
    }

    #[test]
    fn computes_partition_estimates_and_skew_metrics() {
        let config = example_config();
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            ["a", "a", "a", "b", "b", "c", "d", "e"]
                .into_iter()
                .map(String::from),
        ));

        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");
        let estimates = &statistics.metadata.estimates.before_partition_sizes;
        let skew = &statistics.metadata.skew;

        assert_eq!(statistics.metadata.estimates.target_partitions, 4);
        assert_eq!(estimates.len(), 4);
        assert_eq!(estimates.iter().sum::<u64>(), 8);
        assert_eq!(skew.max_partition_size, *estimates.iter().max().unwrap());
        assert_eq!(skew.mean_partition_size, 2.0);
        assert!(skew.max_mean_imbalance_ratio >= 1.0);
        assert!(skew.partition_size_variance >= 0.0);
        assert!(skew.coefficient_of_variation >= 0.0);
    }

    #[test]
    fn small_dataset_does_not_exceed_memory_limit() {
        let config = example_config();
        let dataset = input_dataset_with_file_size(
            Dataset::from_key_values("user_id", ["a", "b"]),
            1024 * 1024,
        );

        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");

        assert_eq!(
            statistics.metadata.resources.configured_memory_limit_mb,
            4096
        );
        assert_eq!(
            statistics.metadata.resources.estimated_dataset_size_mb,
            Some(1)
        );
        assert!(statistics.metadata.resources.in_memory_processing_used);
        assert!(!statistics.metadata.resources.memory_limit_exceeded);
        assert!(statistics.metadata.resources.warnings.is_empty());
    }

    #[test]
    fn memory_limit_warning_allows_execution_when_not_strict() {
        let config = config_with_memory_guard(1, false);
        let dataset = input_dataset_with_file_size(
            Dataset::from_key_values("user_id", ["a", "b"]),
            2 * 1024 * 1024,
        );

        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");

        assert_eq!(
            statistics.metadata.resources.estimated_dataset_size_mb,
            Some(2)
        );
        assert!(statistics.metadata.resources.memory_limit_exceeded);
        assert_eq!(
            statistics.metadata.resources.warnings,
            vec!["estimated_dataset_size_exceeds_configured_memory_limit".to_string()]
        );
    }

    #[test]
    fn strict_memory_limit_returns_error_when_estimate_exceeds_limit() {
        let config = config_with_memory_guard(1, true);
        let dataset = input_dataset_with_file_size(
            Dataset::from_key_values("user_id", ["a", "b"]),
            2 * 1024 * 1024,
        );

        let error =
            compute_statistics(&config, &dataset).expect_err("strict memory guard should fail");

        assert!(matches!(
            error,
            Error::ResourceLimitExceeded {
                configured_memory_limit_mb: 1,
                estimated_dataset_size_mb: 2,
            }
        ));
    }

    #[test]
    fn small_dataset_uses_adaptive_partition_count_from_size_estimate() {
        let config = example_config();
        let dataset = InputDataset {
            path: "<memory>".to_string(),
            format: DatasetFormat::Parquet,
            files: vec![InputFile {
                path: "input.parquet".to_string(),
                size_bytes: 1024,
            }],
            rows: Dataset::from_key_values(
                "user_id",
                ["a", "a", "b", "b", "c", "c", "d", "d"]
                    .into_iter()
                    .map(String::from),
            ),
            batches: Vec::new(),
        };

        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");

        assert_eq!(statistics.metadata.estimates.target_partitions, 1);
        assert_eq!(
            statistics.metadata.estimates.before_partition_sizes,
            vec![dataset.rows.row_count()]
        );
    }

    #[test]
    fn final_heavy_keys_include_keys_that_only_exceed_target_partition_rows() {
        let config = Config::from_yaml_str(
            r#"
dataset:
  input: "./data/input.parquet"
  output: "./data/output_partitioned"
  format: "parquet"

partitioning:
  key_columns: ["user_id"]
  target_partition_size_mb: 1
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
        .expect("config should parse");
        let rows = Dataset::from_key_values(
            "user_id",
            std::iter::repeat_n("a", 2500)
                .chain(std::iter::repeat_n("b", 2500))
                .chain(std::iter::repeat_n("c", 2500))
                .chain(std::iter::repeat_n("d", 2500)),
        );
        let dataset = InputDataset {
            path: "<memory>".to_string(),
            format: DatasetFormat::Parquet,
            files: vec![InputFile {
                path: "input.parquet".to_string(),
                size_bytes: 104_850_000,
            }],
            rows,
            batches: Vec::new(),
        };

        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");

        assert_eq!(statistics.metadata.input.mean_key_frequency, 2500.0);
        assert_eq!(statistics.metadata.input.heavy_hitter_candidates.len(), 0);
        assert_eq!(statistics.metadata.input.heavy_hitters.len(), 4);
        assert!(statistics.metadata.input.heavy_hitters.iter().all(|heavy| {
            heavy.detection_reasons
                == vec![crate::manifest::HeavyKeyReason::ExceedsTargetPartitionRows]
        }));
    }

    #[test]
    fn before_partition_sizes_sum_to_row_count_for_uniform_data() {
        let config = example_config();
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            ["a", "a", "b", "b", "c", "c", "d", "d"]
                .into_iter()
                .map(String::from),
        ));

        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");

        assert!(statistics.metadata.input.heavy_hitters.is_empty());
        assert_eq!(
            statistics
                .metadata
                .estimates
                .before_partition_sizes
                .iter()
                .sum::<u64>(),
            dataset.rows.row_count()
        );
    }

    fn config_with_memory_guard(memory_limit_mb: u64, fail_on_memory_limit: bool) -> Config {
        Config::from_yaml_str(&format!(
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
  memory_limit_mb: {memory_limit_mb}
  fail_on_memory_limit: {fail_on_memory_limit}
"#
        ))
        .expect("test config should parse")
    }

    fn input_dataset_with_file_size(rows: Dataset, size_bytes: u64) -> InputDataset {
        InputDataset {
            path: "input.parquet".to_string(),
            format: DatasetFormat::Parquet,
            files: vec![InputFile {
                path: "input.parquet".to_string(),
                size_bytes,
            }],
            rows,
            batches: Vec::new(),
        }
    }
}
