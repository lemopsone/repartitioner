use std::collections::BTreeMap;

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
    let key_frequencies = BTreeMap::new();
    let heavy_hitters =
        heavy_hitters::detect_heavy_hitters(&key_frequencies, config.partitioning.heavy_key_alpha)
            .into_iter()
            .map(|heavy| HeavyKeyPlan {
                key: heavy.key,
                estimated_frequency: heavy.frequency,
                salt_count: 1,
            })
            .collect();

    let metadata = StatsMetadata {
        version: METADATA_VERSION.to_string(),
        input: InputStats {
            total_rows: 0,
            input_files: dataset
                .files
                .iter()
                .map(|file| InputFileStats {
                    path: file.path.clone(),
                    size_bytes: file.size_bytes,
                })
                .collect(),
            estimated_row_width_bytes: None,
            distinct_keys: None,
            key_frequencies,
            heavy_hitters,
        },
        skew: SkewStats {
            max_partition_size: 0,
            mean_partition_size: 0.0,
            median_partition_size: 0.0,
            p95_partition_size: 0.0,
            partition_size_variance: 0.0,
            coefficient_of_variation: 0.0,
            max_mean_imbalance_ratio: 0.0,
        },
        estimates: PartitionEstimates {
            target_partitions: config.partitioning.max_partitions,
            before_partition_sizes: Vec::new(),
            after_partition_sizes: vec![0; config.partitioning.max_partitions],
        },
    };

    Ok(ComputedStatistics { metadata })
}
