use std::{
    collections::BTreeSet,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    manifest::{HeavyKeyPlan, NormalKeyPlan, PartitionPlan, SaltPartitionPlan, METADATA_VERSION},
    statistics::ComputedStatistics,
    Config, Result,
};

#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub metadata: PartitionPlan,
}

pub fn build_plan(config: &Config, statistics: &ComputedStatistics) -> Result<Plan> {
    let output_partitions = config.partitioning.max_partitions.get();
    let target_partition_rows =
        target_partition_rows(statistics.metadata.input.total_rows, output_partitions);
    let heavy_key_names: BTreeSet<_> = statistics
        .metadata
        .input
        .heavy_hitters
        .iter()
        .map(|heavy| heavy.key.as_str())
        .collect();

    let normal_keys = statistics
        .metadata
        .input
        .key_frequencies
        .iter()
        .filter(|(key, _)| !heavy_key_names.contains(key.as_str()))
        .map(|(key, frequency)| NormalKeyPlan {
            key: key.clone(),
            estimated_frequency: *frequency,
            partition_id: hash_partition(key, output_partitions, config.partitioning.seed),
        })
        .collect();

    let heavy_keys = statistics
        .metadata
        .input
        .heavy_hitters
        .iter()
        .map(|heavy| {
            let salt_count = salt_count(heavy.estimated_frequency, target_partition_rows);
            let salt_partitions = (0..salt_count)
                .map(|salt_index| SaltPartitionPlan {
                    salt_index,
                    partition_id: salted_hash_partition(
                        &heavy.key,
                        salt_index,
                        output_partitions,
                        config.partitioning.seed,
                    ),
                })
                .collect();

            HeavyKeyPlan {
                key: heavy.key.clone(),
                estimated_frequency: heavy.estimated_frequency,
                salt_count,
                salt_partitions,
            }
        })
        .collect();

    Ok(Plan {
        metadata: PartitionPlan {
            version: METADATA_VERSION.to_string(),
            created_at: creation_timestamp(),
            strategy: config.partitioning.strategy.clone(),
            key_columns: config.partitioning.key_columns.clone(),
            target_partition_size_mb: config.partitioning.target_partition_size_mb.get(),
            target_partition_rows,
            output_partitions,
            normal_keys,
            heavy_keys,
            hash_function: "fnv1a64_seeded".to_string(),
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

pub(crate) fn hash_partition(key: &str, output_partitions: usize, seed: u64) -> usize {
    if output_partitions == 0 {
        return 0;
    }

    (fnv1a64_seeded(seed, key.as_bytes()) as usize) % output_partitions
}

fn salted_hash_partition(
    key: &str,
    salt_index: usize,
    output_partitions: usize,
    seed: u64,
) -> usize {
    if output_partitions == 0 {
        return 0;
    }

    let salted_key = format!("{key}|salt={salt_index}");
    hash_partition(&salted_key, output_partitions, seed)
}

fn fnv1a64_seeded(seed: u64, bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64 ^ seed;

    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }

    hash
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
        assert_eq!(plan.metadata.heavy_keys[0].salt_partitions.len(), 3);
        assert!(plan.metadata.heavy_keys[0]
            .salt_partitions
            .iter()
            .all(|salt| salt.partition_id < plan.metadata.output_partitions));
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
        assert_eq!(plan.metadata.normal_keys.len(), 4);
        assert!(plan
            .metadata
            .normal_keys
            .iter()
            .all(|key| key.partition_id < plan.metadata.output_partitions));
    }

    #[test]
    fn assigns_normal_keys_to_deterministic_hash_partitions() {
        let config = example_config();
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            ["a", "a", "b", "b", "c", "c", "d", "d"]
                .into_iter()
                .map(String::from),
        ));
        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");

        let first_plan = build_plan(&config, &statistics).expect("first plan should build");
        let second_plan = build_plan(&config, &statistics).expect("second plan should build");

        assert_eq!(
            first_plan.metadata.normal_keys,
            second_plan.metadata.normal_keys
        );
        assert_eq!(first_plan.metadata.hash_function, "fnv1a64_seeded");
    }

    #[test]
    fn produces_serializable_partition_plan() {
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

        let json = serde_json::to_string(&plan.metadata).expect("plan should serialize");

        assert!(json.contains("\"normal_keys\""));
        assert!(json.contains("\"salt_partitions\""));
        assert!(json.contains("\"target_partition_rows\":4"));
    }
}
