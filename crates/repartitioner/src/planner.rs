use std::time::{SystemTime, UNIX_EPOCH};

use crate::{
    manifest::{HeavyKeyPlan, PartitionPlan, METADATA_VERSION},
    statistics::ComputedStatistics,
    Config, Result,
};

#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub metadata: PartitionPlan,
}

pub fn build_plan(config: &Config, statistics: &ComputedStatistics) -> Result<Plan> {
    let target_partition_rows = target_partition_rows(
        statistics.metadata.input.total_rows,
        config.partitioning.max_partitions.get(),
    );
    let heavy_keys = statistics
        .metadata
        .input
        .heavy_hitters
        .iter()
        .map(|heavy| HeavyKeyPlan {
            key: heavy.key.clone(),
            estimated_frequency: heavy.estimated_frequency,
            salt_count: salt_count(heavy.estimated_frequency, target_partition_rows),
        })
        .collect();

    Ok(Plan {
        metadata: PartitionPlan {
            version: METADATA_VERSION.to_string(),
            created_at: creation_timestamp(),
            strategy: config.partitioning.strategy.clone(),
            key_columns: config.partitioning.key_columns.clone(),
            target_partition_size_mb: config.partitioning.target_partition_size_mb.get(),
            output_partitions: config.partitioning.max_partitions.get(),
            heavy_keys,
            hash_function: "siphash-1-3-default-hasher-placeholder".to_string(),
            seed: config.partitioning.seed,
        },
    })
}

fn target_partition_rows(total_rows: u64, output_partitions: usize) -> u64 {
    if total_rows == 0 || output_partitions == 0 {
        return 1;
    }

    total_rows.div_ceil(output_partitions as u64).max(1)
}

fn salt_count(frequency: u64, target_partition_rows: u64) -> usize {
    frequency.div_ceil(target_partition_rows.max(1)).max(1) as usize
}

fn creation_timestamp() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();

    format!("unix_seconds:{seconds}")
}

#[cfg(test)]
mod tests {
    use crate::{
        dataset::Dataset, reader::InputDataset, statistics::compute_statistics,
        tests::example_config,
    };

    use super::*;

    #[test]
    fn plans_salt_buckets_for_heavy_key() {
        let config = example_config();
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            [
                "heavy", "heavy", "heavy", "heavy", "heavy", "heavy", "heavy", "heavy", "heavy",
                "heavy", "a", "b", "c", "d",
            ]
            .into_iter()
            .map(String::from),
        ));
        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");

        let plan = build_plan(&config, &statistics).expect("plan should build");

        assert_eq!(plan.metadata.output_partitions, 4);
        assert_eq!(plan.metadata.heavy_keys.len(), 1);
        assert_eq!(plan.metadata.heavy_keys[0].key, "user_id=heavy");
        assert_eq!(plan.metadata.heavy_keys[0].estimated_frequency, 10);
        assert_eq!(plan.metadata.heavy_keys[0].salt_count, 3);
    }

    #[test]
    fn keeps_uniform_data_unsalted() {
        let config = example_config();
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            ["a", "a", "b", "b", "c", "c", "d", "d"]
                .into_iter()
                .map(String::from),
        ));
        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");

        let plan = build_plan(&config, &statistics).expect("plan should build");

        assert!(plan.metadata.heavy_keys.is_empty());
    }
}
