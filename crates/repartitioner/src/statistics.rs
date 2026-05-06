use std::collections::BTreeMap;

use crate::{
    config::HeavyHitterMode,
    dataset::Row,
    hashing, heavy_hitters,
    key_encoding::{key_value_to_string, key_value_type_name},
    manifest::{
        HeavyHitterDetectionMetadata, HeavyKeyPlan, InputFileStats, InputStats, JoinSideStatistics,
        JoinStatisticsMetadata, PartitionBoundMetadata, PartitionEstimates, PlanKey, PlanKeyPart,
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
        let after_skew = skew_stats(&partition_sizes);
        self.metadata.estimates.after_partition_sizes = partition_sizes;
        self.metadata.after_skew = Some(after_skew);
        self.metadata
            .partition_bound
            .set_after(self.metadata.estimates.after_partition_sizes.as_slice());
    }

    pub fn set_timing(&mut self, timing: TimingMetadata) {
        self.metadata.timing = Some(timing);
    }

    pub fn set_join_statistics(&mut self, join: JoinStatisticsMetadata) {
        self.metadata.join = Some(join);
    }
}

pub fn compute_statistics(config: &Config, dataset: &InputDataset) -> Result<ComputedStatistics> {
    let resources = resource_estimate(config, dataset)?;
    let file_size_summary = file_size_summary(config, dataset);
    let key_frequency_summary = key_frequency_summary(dataset, config);
    let key_frequencies_exact = key_frequency_summary.key_frequencies_exact;
    let key_frequencies_truncated = key_frequency_summary.key_frequencies_truncated;
    let normal_keys_materialized = key_frequency_summary.normal_keys_materialized;
    let distinct_keys =
        key_frequencies_exact.then_some(key_frequency_summary.frequencies.len() as u64);
    let key_frequencies = key_frequency_summary.frequencies;
    let key_values = key_value_map(dataset, &config.partitioning.key_columns);
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
    .map(|heavy| heavy_key_plan_placeholder(heavy, &key_values))
    .collect();
    let heavy_hitters = heavy_hitters::detect_final_heavy_keys(
        &key_frequencies,
        config.partitioning.heavy_key_alpha,
        target_partitioning.target_partition_rows,
    )
    .into_iter()
    .map(|heavy| heavy_key_plan_placeholder(heavy, &key_values))
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
            distinct_keys,
            key_frequencies_exact,
            key_frequencies_truncated,
            normal_keys_materialized,
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
        join: None,
        before_skew: skew.clone(),
        after_skew: None,
        skew,
        partition_bound: PartitionBoundMetadata::new(
            target_partitioning.target_partition_rows,
            partition_sizes.as_slice(),
        ),
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

pub fn build_join_statistics(
    join_keys: Vec<String>,
    left: &ComputedStatistics,
    right: Option<&ComputedStatistics>,
) -> JoinStatisticsMetadata {
    JoinStatisticsMetadata {
        join_keys,
        left: join_side_statistics(left),
        right: right.map(join_side_statistics),
    }
}

fn join_side_statistics(statistics: &ComputedStatistics) -> JoinSideStatistics {
    let total_size_bytes = total_input_size_bytes(&statistics.metadata.input.input_files);
    JoinSideStatistics {
        total_rows: statistics.metadata.input.total_rows,
        total_size_bytes,
        estimated_size_mb: total_size_bytes.map(|bytes| bytes.div_ceil(1024 * 1024)),
        heavy_keys: statistics
            .metadata
            .input
            .heavy_hitters
            .iter()
            .map(|heavy| heavy.key.clone())
            .collect(),
        heavy_key_values: statistics
            .metadata
            .input
            .heavy_hitters
            .iter()
            .filter_map(|heavy| heavy.structured_key.clone())
            .collect(),
    }
}

fn total_input_size_bytes(input_files: &[InputFileStats]) -> Option<u64> {
    let total = input_files.iter().map(|file| file.size_bytes).sum::<u64>();

    (total > 0).then_some(total)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KeyFrequencySummary {
    frequencies: BTreeMap<String, u64>,
    detection_metadata: HeavyHitterDetectionMetadata,
    key_frequencies_exact: bool,
    key_frequencies_truncated: bool,
    normal_keys_materialized: bool,
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

fn heavy_key_plan_placeholder(
    heavy: heavy_hitters::HeavyHitter,
    key_values: &BTreeMap<String, PlanKey>,
) -> HeavyKeyPlan {
    HeavyKeyPlan {
        structured_key: key_values.get(&heavy.key).cloned(),
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
    let observed_total_rows = dataset.rows.row_count();
    match config.statistics.heavy_hitter_mode {
        HeavyHitterMode::Exact => {
            let frequencies = key_frequencies(dataset, &config.partitioning.key_columns);
            KeyFrequencySummary {
                detection_metadata: HeavyHitterDetectionMetadata {
                    mode: "exact".to_string(),
                    capacity: config.statistics.approximate_capacity.get(),
                    error_bound: "0".to_string(),
                    exact: true,
                    frequencies_truncated: false,
                    summary_size: frequencies.len(),
                    observed_total_rows,
                    max_error: Some(0),
                },
                frequencies,
                key_frequencies_exact: true,
                key_frequencies_truncated: false,
                normal_keys_materialized: true,
            }
        }
        HeavyHitterMode::Approximate => {
            let summary = heavy_hitters::space_saving(
                dataset
                    .rows
                    .rows
                    .iter()
                    .filter_map(|row| row.partition_key(&config.partitioning.key_columns)),
                config.statistics.approximate_capacity.get(),
            );
            let summary_size = summary.frequencies.len();
            KeyFrequencySummary {
                frequencies: summary.frequencies,
                detection_metadata: HeavyHitterDetectionMetadata {
                    mode: "approximate".to_string(),
                    capacity: config.statistics.approximate_capacity.get(),
                    error_bound: format!("space_saving_max_overestimate={}", summary.max_error),
                    exact: false,
                    frequencies_truncated: true,
                    summary_size,
                    observed_total_rows,
                    max_error: Some(summary.max_error),
                },
                key_frequencies_exact: false,
                key_frequencies_truncated: true,
                normal_keys_materialized: false,
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

fn key_value_map(dataset: &InputDataset, key_columns: &[String]) -> BTreeMap<String, PlanKey> {
    let mut values = BTreeMap::new();

    for row in &dataset.rows.rows {
        if let Some(key) = row.partition_key(key_columns) {
            values
                .entry(key.clone())
                .or_insert_with(|| plan_key_from_row(row, key_columns, key));
        }
    }

    values
}

fn plan_key_from_row(row: &Row, key_columns: &[String], encoded: String) -> PlanKey {
    let parts = key_columns
        .iter()
        .filter_map(|column| {
            row.key_values().get(column).map(|value| PlanKeyPart {
                column: column.clone(),
                value_type: key_value_type_name(value).to_string(),
                value: key_value_to_string(value),
            })
        })
        .collect();

    PlanKey { encoded, parts }
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
    fn exact_mode_preserves_current_exact_metadata() {
        let config = example_config();
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            ["a", "a", "b", "c"].into_iter().map(String::from),
        ));

        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");

        assert_eq!(statistics.metadata.input.distinct_keys, Some(3));
        assert!(statistics.metadata.input.key_frequencies_exact);
        assert!(!statistics.metadata.input.key_frequencies_truncated);
        assert!(statistics.metadata.input.normal_keys_materialized);
        assert!(statistics.metadata.heavy_hitter_detection.exact);
        assert!(
            !statistics
                .metadata
                .heavy_hitter_detection
                .frequencies_truncated
        );
        assert_eq!(statistics.metadata.heavy_hitter_detection.summary_size, 3);
        assert_eq!(
            statistics
                .metadata
                .heavy_hitter_detection
                .observed_total_rows,
            4
        );
        assert_eq!(
            statistics.metadata.heavy_hitter_detection.max_error,
            Some(0)
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
        let config = approximate_config(32);
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
        assert!(!statistics.metadata.heavy_hitter_detection.exact);
        assert!(
            statistics
                .metadata
                .heavy_hitter_detection
                .frequencies_truncated
        );
        assert_eq!(statistics.metadata.heavy_hitter_detection.capacity, 32);
        assert_eq!(
            statistics.metadata.heavy_hitter_detection.summary_size,
            statistics.metadata.input.key_frequencies.len()
        );
        assert_eq!(
            statistics
                .metadata
                .heavy_hitter_detection
                .observed_total_rows,
            1000
        );
        assert!(statistics
            .metadata
            .heavy_hitter_detection
            .max_error
            .is_some());
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
    fn approximate_mode_marks_key_frequencies_as_truncated() {
        let config = approximate_config(8);
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            std::iter::repeat_n("heavy".to_string(), 50)
                .chain((0..200).map(|index| format!("key_{index}"))),
        ));

        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");

        assert!(!statistics.metadata.input.key_frequencies_exact);
        assert!(statistics.metadata.input.key_frequencies_truncated);
        assert!(!statistics.metadata.input.normal_keys_materialized);
        assert!(statistics.metadata.input.key_frequencies.len() <= 8);
    }

    #[test]
    fn approximate_mode_sets_distinct_keys_to_none() {
        let config = approximate_config(8);
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            std::iter::repeat_n("heavy".to_string(), 50)
                .chain((0..200).map(|index| format!("key_{index}"))),
        ));

        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");

        assert_eq!(statistics.metadata.input.distinct_keys, None);
    }

    fn approximate_config(capacity: usize) -> Config {
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

statistics:
  heavy_hitter_mode: "approximate"
  approximate_capacity: {capacity}

job:
  type: "group_by"
  downstream_engine: "spark"

resources:
  local_threads: 8
  memory_limit_mb: 4096
"#
        ))
        .expect("config should parse")
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
        assert_eq!(statistics.metadata.before_skew, statistics.metadata.skew);
        assert_eq!(statistics.metadata.after_skew, None);
        assert_eq!(skew.max_partition_size, *estimates.iter().max().unwrap());
        assert_eq!(skew.mean_partition_size, 2.0);
        assert!(skew.max_mean_imbalance_ratio >= 1.0);
        assert!(skew.partition_size_variance >= 0.0);
        assert!(skew.coefficient_of_variation >= 0.0);
    }

    #[test]
    fn stats_contains_before_and_after_skew_for_rewrite() {
        let config = example_config();
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            std::iter::repeat_n("heavy", 40).chain(["a", "b", "c", "d"]),
        ));
        let mut statistics =
            compute_statistics(&config, &dataset).expect("statistics should compute");

        statistics.set_after_partition_sizes(vec![11, 11, 11, 11]);

        assert_eq!(statistics.metadata.before_skew, statistics.metadata.skew);
        assert!(statistics.metadata.after_skew.is_some());
        assert_eq!(
            statistics
                .metadata
                .after_skew
                .as_ref()
                .unwrap()
                .max_partition_size,
            11
        );
    }

    #[test]
    fn stats_after_skew_equals_before_skew_for_no_op() {
        let config = example_config();
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            ["a", "a", "b", "b", "c", "c", "d", "d"]
                .into_iter()
                .map(String::from),
        ));
        let mut statistics =
            compute_statistics(&config, &dataset).expect("statistics should compute");

        statistics.set_after_partition_sizes(
            statistics.metadata.estimates.before_partition_sizes.clone(),
        );

        assert_eq!(
            statistics.metadata.after_skew.as_ref(),
            Some(&statistics.metadata.before_skew)
        );
    }

    #[test]
    fn partition_bound_reports_unsatisfied_when_after_max_exceeds_target() {
        let config = config_with_target_size(1);
        let dataset = input_dataset_with_file_size(
            Dataset::from_key_values("user_id", (0..20).map(|index| format!("key_{index}"))),
            20 * 1024 * 1024,
        );
        let mut statistics =
            compute_statistics(&config, &dataset).expect("statistics should compute");

        statistics.set_after_partition_sizes(vec![20, 0, 0, 0]);

        assert_eq!(statistics.metadata.partition_bound.target_partition_rows, 1);
        assert_eq!(
            statistics
                .metadata
                .partition_bound
                .target_rows_satisfied_after,
            Some(false)
        );
        assert_eq!(
            statistics.metadata.partition_bound.reason.as_deref(),
            Some("after_max_partition_rows_exceed_target")
        );
    }

    #[test]
    fn partition_bound_reports_satisfied_when_after_max_within_target() {
        let config = config_with_target_size(1);
        let dataset = input_dataset_with_file_size(
            Dataset::from_key_values("user_id", (0..20).map(|index| format!("key_{index}"))),
            20 * 1024 * 1024,
        );
        let mut statistics =
            compute_statistics(&config, &dataset).expect("statistics should compute");

        statistics.set_after_partition_sizes(vec![1; 20]);

        assert_eq!(
            statistics
                .metadata
                .partition_bound
                .target_rows_satisfied_after,
            Some(true)
        );
        assert_eq!(statistics.metadata.partition_bound.reason, None);
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

    fn config_with_target_size(target_partition_size_mb: u64) -> Config {
        Config::from_yaml_str(&format!(
            r#"
dataset:
  input: "./data/input.parquet"
  output: "./data/output_partitioned"
  format: "parquet"

partitioning:
  key_columns: ["user_id"]
  target_partition_size_mb: {target_partition_size_mb}
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
