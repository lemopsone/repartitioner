use std::collections::BTreeMap;

use crate::{hashing, planner::Plan, reader::InputDataset, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionAssignmentSummary {
    pub partition_row_counts: Vec<u64>,
    pub records: Vec<RecordPartitionAssignment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordPartitionAssignment {
    pub row_index: usize,
    pub key: Option<String>,
    pub partition_id: usize,
    pub salt_index: Option<usize>,
}

pub fn assign_partitions(
    plan: &Plan,
    dataset: &InputDataset,
) -> Result<PartitionAssignmentSummary> {
    let mut partition_row_counts = vec![0; plan.metadata.output_partitions];
    let mut records = Vec::with_capacity(dataset.rows.rows.len());
    let normal_partitions = normal_partition_lookup(plan);
    let heavy_partitions = heavy_partition_lookup(plan);
    let mut heavy_occurrences = BTreeMap::<String, usize>::new();

    for (row_index, row) in dataset.rows.rows.iter().enumerate() {
        let key = row.partition_key(&plan.metadata.key_columns);
        let (partition_id, salt_index) = match key.as_deref() {
            Some(key) => match heavy_partitions.get(key) {
                Some(salt_partitions) => {
                    let occurrence_index = heavy_occurrences.entry(key.to_string()).or_insert(0);
                    let salt_index = *occurrence_index % salt_partitions.len().max(1);
                    *occurrence_index += 1;
                    let partition_id =
                        salt_partitions
                            .get(&salt_index)
                            .copied()
                            .unwrap_or_else(|| {
                                hashing::partition_id(
                                    key,
                                    plan.metadata.output_partitions,
                                    plan.metadata.seed,
                                )
                            });
                    (partition_id, Some(salt_index))
                }
                None => {
                    let partition_id = normal_partitions.get(key).copied().unwrap_or_else(|| {
                        hashing::partition_id(
                            key,
                            plan.metadata.output_partitions,
                            plan.metadata.seed,
                        )
                    });
                    (partition_id, None)
                }
            },
            None => (
                hashing::partition_id(
                    "<missing_partition_key>",
                    plan.metadata.output_partitions,
                    plan.metadata.seed,
                ),
                None,
            ),
        };

        if let Some(row_count) = partition_row_counts.get_mut(partition_id) {
            *row_count += 1;
        }

        records.push(RecordPartitionAssignment {
            row_index,
            key,
            partition_id,
            salt_index,
        });
    }

    Ok(PartitionAssignmentSummary {
        partition_row_counts,
        records,
    })
}

fn normal_partition_lookup(plan: &Plan) -> BTreeMap<&str, usize> {
    plan.metadata
        .normal_keys
        .iter()
        .map(|key| (key.key.as_str(), key.partition_id))
        .collect()
}

fn heavy_partition_lookup(plan: &Plan) -> BTreeMap<&str, BTreeMap<usize, usize>> {
    plan.metadata
        .heavy_keys
        .iter()
        .map(|key| {
            let salt_partitions = key
                .salt_partitions
                .iter()
                .map(|salt| (salt.salt_index, salt.partition_id))
                .collect();
            (key.key.as_str(), salt_partitions)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::{
        dataset::{Dataset, Row},
        hashing,
        key_encoding::KeyValue,
        planner::build_plan,
        reader::InputDataset,
        statistics::compute_statistics,
        tests::example_config,
        Config,
    };

    use super::*;

    #[test]
    fn assigns_records_deterministically() {
        let config = example_config();
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            ["heavy", "heavy", "heavy", "heavy", "a", "b", "c", "d"],
        ));
        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");
        let plan = build_plan(&config, &statistics).expect("plan should build");

        let first = assign_partitions(&plan, &dataset).expect("first assignment should work");
        let second = assign_partitions(&plan, &dataset).expect("second assignment should work");

        assert_eq!(first, second);
        assert_eq!(first.records.len(), 8);
        assert_eq!(first.partition_row_counts.iter().sum::<u64>(), 8);
    }

    #[test]
    fn assigns_heavy_key_records_across_salt_partitions() {
        let config = example_config();
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            [
                "heavy", "heavy", "heavy", "heavy", "heavy", "heavy", "heavy", "heavy", "heavy",
                "heavy", "a", "b", "c", "d",
            ],
        ));
        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");
        let plan = build_plan(&config, &statistics).expect("plan should build");

        let assignments = assign_partitions(&plan, &dataset).expect("assignment should work");
        let heavy_salt_indexes: Vec<_> = assignments
            .records
            .iter()
            .filter(|record| record.key.as_deref() == Some("7:user_id#utf8:5:heavy"))
            .filter_map(|record| record.salt_index)
            .collect();

        assert_eq!(heavy_salt_indexes.len(), 10);
        assert_eq!(&heavy_salt_indexes[0..6], &[0, 1, 2, 0, 1, 2]);
    }

    #[test]
    fn skewed_data_has_lower_max_mean_imbalance_after_adaptive_partitioning() {
        let config = example_config();
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            std::iter::repeat_n("heavy", 40).chain(["a", "b", "c", "d", "e", "f", "g", "h"]),
        ));
        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");
        let plan = build_plan(&config, &statistics).expect("plan should build");

        let adaptive = assign_partitions(&plan, &dataset).expect("assignment should work");
        let before_ratio =
            max_mean_imbalance_ratio(&statistics.metadata.estimates.before_partition_sizes);
        let after_ratio = max_mean_imbalance_ratio(&adaptive.partition_row_counts);

        assert!(after_ratio < before_ratio);
    }

    #[test]
    fn partitioner_uses_normal_key_plan_not_hash_fallback() {
        let config = config_with_load_aware_normal_keys();
        let values = normal_key_values_for_hash_partition(0, 12);
        let dataset = InputDataset::from_rows(Dataset::from_key_values("user_id", values));
        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");
        let plan = build_plan(&config, &statistics).expect("plan should build");

        let assignments = assign_partitions(&plan, &dataset).expect("assignment should work");
        let normal_lookup = normal_partition_lookup(&plan);

        assert!(plan.metadata.normal_keys.iter().any(|normal| {
            normal.partition_id
                != hashing::partition_id(
                    &normal.key,
                    plan.metadata.output_partitions,
                    plan.metadata.seed,
                )
        }));
        assert!(assignments.records.iter().all(|record| {
            let Some(key) = record.key.as_deref() else {
                return false;
            };
            normal_lookup.get(key).copied() == Some(record.partition_id)
        }));
    }

    fn max_mean_imbalance_ratio(partition_sizes: &[u64]) -> f64 {
        if partition_sizes.is_empty() {
            return 0.0;
        }

        let mean = partition_sizes.iter().sum::<u64>() as f64 / partition_sizes.len() as f64;
        if mean == 0.0 {
            return 0.0;
        }

        *partition_sizes.iter().max().unwrap_or(&0) as f64 / mean
    }

    fn config_with_load_aware_normal_keys() -> Config {
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
  normal_key_assignment: "load_aware"
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
        .expect("test config should parse")
    }

    fn normal_key_values_for_hash_partition(partition_id: usize, count: usize) -> Vec<String> {
        let mut values = Vec::new();
        let mut candidate = 0;
        while values.len() < count {
            let value = format!("normal_{candidate}");
            let row = Row::from_key_value("user_id", KeyValue::Utf8(value.clone()));
            let key = row
                .partition_key(&["user_id".to_string()])
                .expect("row should have partition key");
            if hashing::partition_id(&key, 4, 42) == partition_id {
                values.push(value);
            }
            candidate += 1;
        }
        values
    }
}
