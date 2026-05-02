use std::{
    collections::{hash_map::DefaultHasher, BTreeMap},
    hash::{Hash, Hasher},
};

use crate::{
    heavy_hitters,
    manifest::{
        HeavyKeyPlan, InputFileStats, InputStats, PartitionEstimates, SkewStats, StatsMetadata,
        METADATA_VERSION,
    },
    reader::InputDataset,
    Config, Result,
};

#[derive(Debug, Clone, PartialEq)]
pub struct ComputedStatistics {
    pub metadata: StatsMetadata,
}

pub fn compute_statistics(config: &Config, dataset: &InputDataset) -> Result<ComputedStatistics> {
    let key_frequencies = key_frequencies(dataset, &config.partitioning.key_columns);
    let mean_key_frequency = mean_frequency(&key_frequencies);
    let max_key_frequency = max_frequency(&key_frequencies);
    let heavy_hitter_candidates: Vec<_> =
        heavy_hitters::detect_heavy_hitters(&key_frequencies, config.partitioning.heavy_key_alpha)
            .into_iter()
            .map(|heavy| HeavyKeyPlan {
                key: heavy.key,
                estimated_frequency: heavy.frequency,
                salt_count: 1,
                salt_partitions: Vec::new(),
            })
            .collect();
    let partition_sizes = base_partition_sizes(
        dataset,
        &config.partitioning.key_columns,
        config.partitioning.max_partitions.get(),
        config.partitioning.seed,
    );
    let skew = skew_stats(&partition_sizes);

    let metadata = StatsMetadata {
        version: METADATA_VERSION.to_string(),
        input: InputStats {
            total_rows: dataset.rows.row_count(),
            input_files: dataset
                .files
                .iter()
                .map(|file| InputFileStats {
                    path: file.path.clone(),
                    size_bytes: file.size_bytes,
                })
                .collect(),
            estimated_row_width_bytes: None,
            distinct_keys: Some(key_frequencies.len() as u64),
            mean_key_frequency,
            max_key_frequency,
            key_frequencies,
            heavy_hitter_candidates: heavy_hitter_candidates.clone(),
            heavy_hitters: heavy_hitter_candidates,
        },
        skew,
        estimates: PartitionEstimates {
            target_partitions: config.partitioning.max_partitions.get(),
            before_partition_sizes: partition_sizes,
            after_partition_sizes: vec![0; config.partitioning.max_partitions.get()],
        },
    };

    Ok(ComputedStatistics { metadata })
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
            let partition_id = stable_partition(&key, partition_count, seed);
            sizes[partition_id] += 1;
        }
    }

    sizes
}

fn stable_partition(key: &str, partition_count: usize, seed: u64) -> usize {
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    key.hash(&mut hasher);
    (hasher.finish() as usize) % partition_count
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
    use crate::{dataset::Dataset, reader::InputDataset, tests::example_config};

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
            statistics.metadata.input.key_frequencies.get("user_id=a"),
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
            "user_id=heavy"
        );
        assert_eq!(
            statistics.metadata.input.heavy_hitters[0].estimated_frequency,
            5
        );
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
}
