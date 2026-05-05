use std::{
    collections::BTreeSet,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    hashing,
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
    let target_partition_rows = target_partition_rows(config, statistics, output_partitions);
    let heavy_key_names: BTreeSet<_> = statistics
        .metadata
        .input
        .heavy_hitters
        .iter()
        .map(|heavy| heavy.key.as_str())
        .collect();

    let mut estimated_partition_loads = vec![0_u64; output_partitions];
    let normal_keys = statistics
        .metadata
        .input
        .key_frequencies
        .iter()
        .filter(|(key, _)| !heavy_key_names.contains(key.as_str()))
        .map(|(key, frequency)| {
            let partition_id =
                hashing::partition_id(key, output_partitions, config.partitioning.seed);
            if let Some(load) = estimated_partition_loads.get_mut(partition_id) {
                *load += *frequency;
            }

            NormalKeyPlan {
                key: key.clone(),
                estimated_frequency: *frequency,
                partition_id,
            }
        })
        .collect();

    let mut heavy_keys = Vec::new();
    for heavy in &statistics.metadata.input.heavy_hitters {
        let salt_count = salt_count(heavy.estimated_frequency, target_partition_rows);
        let salt_partitions = (0..salt_count)
            .map(|salt_index| {
                let partition_id = least_loaded_salt_partition(
                    &heavy.key,
                    salt_index,
                    output_partitions,
                    config.partitioning.seed,
                    &estimated_partition_loads,
                );
                if let Some(load) = estimated_partition_loads.get_mut(partition_id) {
                    *load += estimated_salt_load(heavy.estimated_frequency, salt_count, salt_index);
                }

                SaltPartitionPlan {
                    salt_index,
                    partition_id,
                }
            })
            .collect();

        heavy_keys.push(HeavyKeyPlan {
            key: heavy.key.clone(),
            estimated_frequency: heavy.estimated_frequency,
            salt_count,
            salt_partitions,
        });
    }

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
            hash_function: hashing::HASH_FUNCTION_NAME.to_string(),
            seed: config.partitioning.seed,
        },
    })
}

fn target_partition_rows(
    config: &Config,
    statistics: &ComputedStatistics,
    output_partitions: usize,
) -> u64 {
    let total_rows = statistics.metadata.input.total_rows;
    if total_rows == 0 || output_partitions == 0 {
        return 1;
    }

    let rows_per_output_partition = total_rows.div_ceil(output_partitions as u64).max(1);
    let Some(row_width_bytes) = statistics.metadata.input.estimated_row_width_bytes else {
        return rows_per_output_partition;
    };

    let target_size_bytes = config
        .partitioning
        .target_partition_size_mb
        .get()
        .saturating_mul(1024 * 1024);
    let rows_per_target_size = (target_size_bytes / row_width_bytes.max(1)).max(1);

    rows_per_output_partition.min(rows_per_target_size).max(1)
}

fn salt_count(frequency: u64, target_partition_rows: u64) -> usize {
    frequency.div_ceil(target_partition_rows.max(1)).max(1) as usize
}

fn least_loaded_salt_partition(
    key: &str,
    salt_index: usize,
    output_partitions: usize,
    seed: u64,
    estimated_partition_loads: &[u64],
) -> usize {
    if output_partitions == 0 {
        return 0;
    }

    (0..output_partitions)
        .min_by_key(|candidate| {
            (
                estimated_partition_loads
                    .get(*candidate)
                    .copied()
                    .unwrap_or_default(),
                salt_candidate_rank(key, salt_index, *candidate, seed),
            )
        })
        .unwrap_or(0)
}

fn salt_candidate_rank(key: &str, salt_index: usize, candidate: usize, seed: u64) -> u64 {
    let candidate_key = format!("{key}|salt={salt_index}|candidate={candidate}");
    hashing::hash_key(seed, &candidate_key)
}

fn estimated_salt_load(frequency: u64, salt_count: usize, salt_index: usize) -> u64 {
    if salt_count == 0 {
        return frequency;
    }

    let base = frequency / salt_count as u64;
    let remainder = frequency % salt_count as u64;
    base + u64::from((salt_index as u64) < remainder)
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
    fn derives_target_rows_from_configured_size_when_row_width_is_known() {
        let config = example_config();
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            [
                "heavy", "heavy", "heavy", "heavy", "heavy", "a", "b", "c", "d", "e",
            ]
            .into_iter()
            .map(String::from),
        ));
        let mut statistics =
            compute_statistics(&config, &dataset).expect("statistics should compute");
        statistics.metadata.input.estimated_row_width_bytes = Some(64 * 1024 * 1024);

        let plan = build_plan(&config, &statistics).expect("plan should build");

        assert_eq!(plan.metadata.target_partition_rows, 2);
        assert_eq!(plan.metadata.heavy_keys[0].salt_count, 3);
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
        assert_eq!(
            first_plan.metadata.hash_function,
            hashing::HASH_FUNCTION_NAME
        );
    }

    #[test]
    fn statistics_before_estimates_use_same_hash_function_as_planner() {
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

        let mut planned_sizes = vec![0_u64; plan.metadata.output_partitions];
        for key in &plan.metadata.normal_keys {
            planned_sizes[key.partition_id] += key.estimated_frequency;
        }

        assert_eq!(
            statistics.metadata.estimates.before_partition_sizes,
            planned_sizes
        );
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
