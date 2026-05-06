use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    config::{JobType, NormalKeyAssignment},
    hashing, heavy_hitters,
    manifest::{
        CostEstimate, HeavyKeyPlan, JoinPlan, NormalKeyPlan, PartitionPlan,
        PartitionPlanFeasibility, PlanAction, PlanKey, RecommendedDownstreamPlan,
        SaltPartitionPlan, TechnicalColumns, METADATA_VERSION,
    },
    statistics::ComputedStatistics,
    targeting, Config, Result,
};

#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub metadata: PartitionPlan,
}

pub fn build_plan(config: &Config, statistics: &ComputedStatistics) -> Result<Plan> {
    let target_partitioning = targeting::compute_target_partitioning(
        config,
        statistics.metadata.input.total_rows,
        statistics.metadata.input.estimated_row_width_bytes,
    );
    let output_partitions = target_partitioning.output_partitions;
    let target_partition_rows = target_partitioning.target_partition_rows;
    let final_heavy_keys = heavy_hitters::detect_final_heavy_keys(
        &statistics.metadata.input.key_frequencies,
        config.partitioning.heavy_key_alpha,
        target_partition_rows,
    );
    let heavy_key_names: BTreeSet<_> = final_heavy_keys
        .iter()
        .map(|heavy| heavy.key.as_str())
        .collect();
    let structured_keys = structured_key_map(statistics);

    let mut estimated_partition_loads = vec![0_u64; output_partitions];
    let mut normal_items = statistics
        .metadata
        .input
        .key_frequencies
        .iter()
        .filter(|(key, _)| !heavy_key_names.contains(key.as_str()))
        .collect::<Vec<_>>();
    normal_items.sort_by(|left, right| right.1.cmp(left.1).then_with(|| left.0.cmp(right.0)));

    let mut normal_keys = Vec::with_capacity(normal_items.len());
    for (key, frequency) in normal_items {
        let partition_id = match config.partitioning.normal_key_assignment {
            NormalKeyAssignment::Hash => {
                hashing::partition_id(key, output_partitions, config.partitioning.seed)
            }
            NormalKeyAssignment::LoadAware => least_loaded_normal_partition(
                key,
                output_partitions,
                config.partitioning.seed,
                &estimated_partition_loads,
            ),
        };
        if let Some(load) = estimated_partition_loads.get_mut(partition_id) {
            *load += *frequency;
        }

        normal_keys.push(NormalKeyPlan {
            key: key.clone(),
            estimated_frequency: *frequency,
            partition_id,
        });
    }

    let mut heavy_keys = Vec::new();
    for heavy in final_heavy_keys {
        let salt_count = salt_count(heavy.frequency, target_partition_rows);
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
                    *load += estimated_salt_load(heavy.frequency, salt_count, salt_index);
                }

                SaltPartitionPlan {
                    salt_index,
                    partition_id,
                }
            })
            .collect();

        heavy_keys.push(HeavyKeyPlan {
            key: heavy.key.clone(),
            structured_key: structured_keys.get(&heavy.key).cloned(),
            estimated_frequency: heavy.frequency,
            detection_reasons: heavy.detection_reasons,
            salt_count,
            salt_partitions,
        });
    }
    let planned_partition_loads = estimated_partition_loads;
    let rewrite_decision = rewrite_decision(
        config,
        statistics,
        target_partition_rows,
        &heavy_keys,
        &planned_partition_loads,
    );
    let cost_estimate = cost_estimate(
        statistics,
        rewrite_decision.rewrite_required,
        rewrite_decision.cost_reason.clone(),
    );
    let join_plan = join_plan(config, statistics);
    let recommended_downstream_plan = recommended_downstream_plan(config, join_plan.as_ref());

    Ok(Plan {
        metadata: PartitionPlan {
            version: METADATA_VERSION.to_string(),
            created_at: creation_timestamp(),
            strategy: config.partitioning.strategy.clone(),
            action: rewrite_decision.action,
            rewrite_required: rewrite_decision.rewrite_required,
            key_columns: config.partitioning.key_columns.clone(),
            job_type: config.job.job_type.clone(),
            downstream_engine: config.job.downstream_engine.clone(),
            target_partition_size_mb: config.partitioning.target_partition_size_mb.get(),
            target_partition_rows,
            min_partitions: config.partitioning.min_partitions.get(),
            max_partitions: config.partitioning.max_partitions.get(),
            output_partitions,
            required_partitions_by_size: target_partitioning.required_partitions_by_size,
            feasibility: PartitionPlanFeasibility {
                target_partition_size_satisfied: target_partitioning.target_size_satisfied,
                reason: target_partitioning.reason,
            },
            technical_columns: TechnicalColumns {
                included: config.output.include_technical_columns,
                partition_column: config.output.partition_column.clone(),
                salt_column: config.output.salt_column.clone(),
                heavy_key_column: config.output.heavy_key_column.clone(),
            },
            normal_key_assignment: config.partitioning.normal_key_assignment.clone(),
            normal_keys,
            heavy_keys,
            recommended_downstream_plan,
            join_plan,
            cost_estimate,
            skip_reason: rewrite_decision.skip_reason,
            hash_function: hashing::HASH_FUNCTION_NAME.to_string(),
            seed: config.partitioning.seed,
        },
    })
}

fn join_plan(config: &Config, statistics: &ComputedStatistics) -> Option<JoinPlan> {
    if config.job.job_type != JobType::Join {
        return None;
    }

    let join = config.join.as_ref()?;
    let join_stats = statistics.metadata.join.as_ref()?;
    let left_heavy_keys = join_stats.left.heavy_keys.clone();
    let right_heavy_keys = join_stats
        .right
        .as_ref()
        .map(|right| right.heavy_keys.clone())
        .unwrap_or_default();
    let right_heavy_set = right_heavy_keys.iter().collect::<HashSet<_>>();
    let shared_heavy_keys = left_heavy_keys
        .iter()
        .filter(|key| right_heavy_set.contains(key))
        .cloned()
        .collect::<Vec<_>>();
    let left_heavy_key_values = join_stats.left.heavy_key_values.clone();
    let right_heavy_key_values = join_stats
        .right
        .as_ref()
        .map(|right| right.heavy_key_values.clone())
        .unwrap_or_default();
    let right_structured_key_set = right_heavy_key_values
        .iter()
        .map(|key| key.encoded.as_str())
        .collect::<HashSet<_>>();
    let shared_heavy_key_values = left_heavy_key_values
        .iter()
        .filter(|key| right_structured_key_set.contains(key.encoded.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let right_side_size_mb = join_stats
        .right
        .as_ref()
        .and_then(|right| right.estimated_size_mb);
    let right_is_broadcastable = join.right_side_mode
        == crate::config::RightSideMode::BroadcastIfSmall
        && right_side_size_mb.is_some_and(|size_mb| size_mb <= join.broadcast_threshold_mb);

    let recommended_strategy = if right_is_broadcastable {
        "broadcast_join_recommendation"
    } else if !shared_heavy_keys.is_empty() {
        "salted_heavy_key_join"
    } else if !left_heavy_keys.is_empty() || !right_heavy_keys.is_empty() {
        "heavy_key_isolation_join"
    } else {
        "physical_only"
    };

    Some(JoinPlan {
        left_heavy_keys,
        right_heavy_keys,
        shared_heavy_keys,
        left_heavy_key_values,
        right_heavy_key_values,
        shared_heavy_key_values,
        recommended_strategy: recommended_strategy.to_string(),
        right_side_size_mb,
        broadcast_threshold_mb: Some(join.broadcast_threshold_mb),
    })
}

fn structured_key_map(statistics: &ComputedStatistics) -> BTreeMap<String, PlanKey> {
    statistics
        .metadata
        .input
        .heavy_hitters
        .iter()
        .chain(statistics.metadata.input.heavy_hitter_candidates.iter())
        .filter_map(|heavy| {
            heavy
                .structured_key
                .clone()
                .map(|structured| (heavy.key.clone(), structured))
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RewriteDecision {
    rewrite_required: bool,
    action: PlanAction,
    skip_reason: Option<String>,
    cost_reason: String,
}

fn rewrite_decision(
    config: &Config,
    statistics: &ComputedStatistics,
    target_partition_rows: u64,
    heavy_keys: &[HeavyKeyPlan],
    planned_partition_loads: &[u64],
) -> RewriteDecision {
    if config.partitioning.force_rewrite {
        return rewrite("force_rewrite");
    }

    if !heavy_keys.is_empty() {
        return rewrite("heavy_keys_detected");
    }

    let skew = &statistics.metadata.skew;
    if skew.max_partition_size > target_partition_rows {
        if !planned_distribution_improves_skew(statistics, planned_partition_loads) {
            return no_rewrite("planned_distribution_does_not_improve_skew");
        }
        return rewrite("max_partition_exceeds_target_rows");
    }

    if skew.max_mean_imbalance_ratio > config.partitioning.no_op_max_imbalance_ratio {
        if !planned_distribution_improves_skew(statistics, planned_partition_loads) {
            return no_rewrite("planned_distribution_does_not_improve_skew");
        }
        return rewrite("imbalance_ratio_exceeds_no_op_threshold");
    }

    no_rewrite("no_rewrite_needed")
}

fn no_rewrite(reason: &str) -> RewriteDecision {
    RewriteDecision {
        rewrite_required: false,
        action: PlanAction::NoOp,
        skip_reason: Some(reason.to_string()),
        cost_reason: reason.to_string(),
    }
}

fn rewrite(reason: &str) -> RewriteDecision {
    RewriteDecision {
        rewrite_required: true,
        action: PlanAction::Rewrite,
        skip_reason: None,
        cost_reason: reason.to_string(),
    }
}

fn planned_distribution_improves_skew(
    statistics: &ComputedStatistics,
    planned_partition_loads: &[u64],
) -> bool {
    let before = &statistics.metadata.estimates.before_partition_sizes;
    if planned_partition_loads.iter().sum::<u64>() != before.iter().sum::<u64>() {
        return true;
    }

    let before_ratio = max_mean_imbalance_ratio(before);
    let after_ratio = max_mean_imbalance_ratio(planned_partition_loads);
    let before_max = before.iter().copied().max().unwrap_or_default();
    let after_max = planned_partition_loads
        .iter()
        .copied()
        .max()
        .unwrap_or_default();

    after_ratio < before_ratio || after_max < before_max
}

fn cost_estimate(
    statistics: &ComputedStatistics,
    rewrite_required: bool,
    reason: String,
) -> CostEstimate {
    if !rewrite_required {
        return CostEstimate {
            estimated_rows_read: 0,
            estimated_rows_written: 0,
            estimated_bytes_read: Some(0),
            estimated_bytes_written: Some(0),
            rewrite_required,
            reason,
        };
    }

    let input_bytes = total_input_size_bytes(statistics);
    CostEstimate {
        estimated_rows_read: statistics.metadata.input.total_rows,
        estimated_rows_written: statistics.metadata.input.total_rows,
        estimated_bytes_read: input_bytes,
        estimated_bytes_written: input_bytes,
        rewrite_required,
        reason,
    }
}

fn recommended_downstream_plan(
    config: &Config,
    join_plan: Option<&JoinPlan>,
) -> RecommendedDownstreamPlan {
    let partition_column = technical_column_name(
        config.output.include_technical_columns,
        &config.output.partition_column,
    );
    let salt_column = technical_column_name(
        config.output.include_technical_columns,
        &config.output.salt_column,
    );
    let heavy_key_column = technical_column_name(
        config.output.include_technical_columns,
        &config.output.heavy_key_column,
    );
    let mut notes = technical_column_notes(config);

    let (strategy, requires_operator_rewrite, partial_group_keys, final_group_keys, join_keys) =
        match &config.job.job_type {
            JobType::Scan => (
                "physical_repartitioning",
                false,
                Vec::new(),
                Vec::new(),
                Vec::new(),
            ),
            JobType::Filter => {
                notes.push("filter_selectivity_unknown".to_string());
                (
                    "physical_repartitioning_with_filter_awareness",
                    false,
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                )
            }
            JobType::GroupBy => {
                if !config.output.include_technical_columns {
                    notes.push("method_aware_group_by_requires_technical_columns".to_string());
                }
                (
                    "two_stage_group_by",
                    true,
                    partial_group_keys(config),
                    config.partitioning.key_columns.clone(),
                    Vec::new(),
                )
            }
            JobType::Join => join_recommendation(config, join_plan, &mut notes),
            JobType::Generic => (
                "physical_repartitioning",
                false,
                Vec::new(),
                Vec::new(),
                Vec::new(),
            ),
        };

    RecommendedDownstreamPlan {
        job_type: config.job.job_type.clone(),
        strategy: strategy.to_string(),
        requires_operator_rewrite,
        partition_column,
        salt_column,
        heavy_key_column,
        partial_group_keys,
        final_group_keys,
        join_keys,
        notes,
    }
}

fn join_recommendation(
    config: &Config,
    join_plan: Option<&JoinPlan>,
    notes: &mut Vec<String>,
) -> (&'static str, bool, Vec<String>, Vec<String>, Vec<String>) {
    let join_keys = config
        .join
        .as_ref()
        .map(|join| join.join_keys.clone())
        .unwrap_or_else(|| config.partitioning.key_columns.clone());

    let Some(join_plan) = join_plan else {
        notes.push("join_plan_missing".to_string());
        return (
            "generic_join_repartitioning",
            true,
            Vec::new(),
            Vec::new(),
            join_keys,
        );
    };

    let (strategy, requires_operator_rewrite, note) = match join_plan.recommended_strategy.as_str()
    {
        "broadcast_join_recommendation" => ("broadcast_join", true, "right_side_broadcastable"),
        "salted_heavy_key_join" => ("salted_heavy_key_join", true, "shared_heavy_keys_detected"),
        "heavy_key_isolation_join" => (
            "heavy_key_isolation_join",
            true,
            "one_sided_heavy_keys_detected",
        ),
        "physical_only" => (
            "physical_repartitioning",
            false,
            "no_join_heavy_keys_detected",
        ),
        _ => {
            notes.push("join_plan_missing".to_string());
            return (
                "generic_join_repartitioning",
                true,
                Vec::new(),
                Vec::new(),
                join_keys,
            );
        }
    };
    notes.push(note.to_string());

    (
        strategy,
        requires_operator_rewrite,
        Vec::new(),
        Vec::new(),
        join_keys,
    )
}

fn technical_column_name(included: bool, column: &str) -> Option<String> {
    included.then(|| column.to_string())
}

fn technical_column_notes(config: &Config) -> Vec<String> {
    if config.output.include_technical_columns {
        Vec::new()
    } else {
        vec!["technical_columns_disabled".to_string()]
    }
}

fn partial_group_keys(config: &Config) -> Vec<String> {
    let mut keys = Vec::new();
    if config.output.include_technical_columns {
        keys.push(config.output.partition_column.clone());
        keys.push(config.output.salt_column.clone());
    }
    keys.extend(config.partitioning.key_columns.clone());
    keys
}

fn total_input_size_bytes(statistics: &ComputedStatistics) -> Option<u64> {
    let total = statistics
        .metadata
        .input
        .input_files
        .iter()
        .map(|file| file.size_bytes)
        .sum::<u64>();

    (total > 0).then_some(total)
}

fn max_mean_imbalance_ratio(partition_sizes: &[u64]) -> f64 {
    if partition_sizes.is_empty() {
        return 0.0;
    }

    let mean = partition_sizes.iter().sum::<u64>() as f64 / partition_sizes.len() as f64;
    if mean == 0.0 {
        return 0.0;
    }

    partition_sizes.iter().copied().max().unwrap_or_default() as f64 / mean
}

fn least_loaded_normal_partition(
    key: &str,
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
                normal_candidate_rank(key, *candidate, seed),
            )
        })
        .unwrap_or(0)
}

fn normal_candidate_rank(key: &str, candidate: usize, seed: u64) -> u64 {
    hashing::hash_key(seed, &format!("{key}|candidate={candidate}"))
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
        config::{DatasetFormat, NormalKeyAssignment},
        dataset::{Dataset, Row},
        key_encoding::KeyValue,
        manifest::PlanAction,
        reader::{InputDataset, InputFile},
        statistics::{build_join_statistics, compute_statistics},
        targeting,
        tests::example_config,
        Config,
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
        assert_eq!(plan.metadata.heavy_keys[0].key, "7:user_id#utf8:5:heavy");
        assert_eq!(plan.metadata.heavy_keys[0].estimated_frequency, 10);
        assert_eq!(plan.metadata.heavy_keys[0].salt_count, 3);
        assert_eq!(plan.metadata.heavy_keys[0].salt_partitions.len(), 3);
        assert!(plan.metadata.heavy_keys[0]
            .salt_partitions
            .iter()
            .all(|salt| salt.partition_id < plan.metadata.output_partitions));
    }

    #[test]
    fn heavy_key_plan_contains_structured_string_key() {
        let config = example_config();
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            std::iter::repeat_n("heavy", 10).chain(["a", "b", "c", "d"]),
        ));
        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");
        let plan = build_plan(&config, &statistics).expect("plan should build");
        let heavy = &plan.metadata.heavy_keys[0];
        let structured_key = heavy
            .structured_key
            .as_ref()
            .expect("structured key should be present");

        assert_eq!(heavy.key, "7:user_id#utf8:5:heavy");
        assert_eq!(structured_key.encoded, heavy.key);
        assert_eq!(structured_key.parts[0].column, "user_id");
        assert_eq!(structured_key.parts[0].value_type, "utf8");
        assert_eq!(structured_key.parts[0].value.as_deref(), Some("heavy"));
    }

    #[test]
    fn heavy_key_plan_contains_structured_int64_key() {
        let config = example_config();
        let rows = std::iter::repeat_n(Row::from_key_value("user_id", KeyValue::Int64(42)), 10)
            .chain([
                Row::from_key_value("user_id", KeyValue::Int64(1)),
                Row::from_key_value("user_id", KeyValue::Int64(2)),
                Row::from_key_value("user_id", KeyValue::Int64(3)),
                Row::from_key_value("user_id", KeyValue::Int64(4)),
            ])
            .collect();
        let dataset = InputDataset::from_rows(Dataset::new(rows));
        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");
        let plan = build_plan(&config, &statistics).expect("plan should build");
        let part = &plan.metadata.heavy_keys[0]
            .structured_key
            .as_ref()
            .unwrap()
            .parts[0];

        assert_eq!(plan.metadata.heavy_keys[0].key, "7:user_id#int64:42");
        assert_eq!(part.value_type, "int64");
        assert_eq!(part.value.as_deref(), Some("42"));
    }

    #[test]
    fn heavy_key_plan_represents_null_key() {
        let config = example_config();
        let rows = std::iter::repeat_n(Row::from_key_value("user_id", KeyValue::Null), 10)
            .chain([
                Row::from_key_value("user_id", KeyValue::Utf8("a".to_string())),
                Row::from_key_value("user_id", KeyValue::Utf8("b".to_string())),
                Row::from_key_value("user_id", KeyValue::Utf8("c".to_string())),
                Row::from_key_value("user_id", KeyValue::Utf8("d".to_string())),
            ])
            .collect();
        let dataset = InputDataset::from_rows(Dataset::new(rows));
        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");
        let plan = build_plan(&config, &statistics).expect("plan should build");
        let part = &plan.metadata.heavy_keys[0]
            .structured_key
            .as_ref()
            .unwrap()
            .parts[0];

        assert_eq!(plan.metadata.heavy_keys[0].key, "7:user_id#null");
        assert_eq!(part.value_type, "null");
        assert_eq!(part.value, None);
    }

    #[test]
    fn encoded_key_field_is_preserved_for_backward_compatibility() {
        let config = example_config();
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            std::iter::repeat_n("heavy", 10).chain(["a", "b", "c", "d"]),
        ));
        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");
        let plan = build_plan(&config, &statistics).expect("plan should build");
        let json = serde_json::to_value(&plan.metadata).expect("plan should serialize");

        assert_eq!(
            json["heavy_keys"][0]["key"].as_str(),
            Some("7:user_id#utf8:5:heavy")
        );
        assert!(json["heavy_keys"][0]["structured_key"].is_object());
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
    fn small_dataset_does_not_use_max_partitions_when_one_partition_is_enough() {
        let config = config_with_partition_limits(1, 128, 128);
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            ["a", "a", "b", "b", "c", "c", "d", "d"]
                .into_iter()
                .map(String::from),
        ));
        let mut statistics =
            compute_statistics(&config, &dataset).expect("statistics should compute");
        statistics.metadata.input.estimated_row_width_bytes = Some(1024);

        let plan = build_plan(&config, &statistics).expect("plan should build");

        assert_eq!(plan.metadata.min_partitions, 1);
        assert_eq!(plan.metadata.max_partitions, 128);
        assert_eq!(plan.metadata.required_partitions_by_size, 1);
        assert_eq!(plan.metadata.output_partitions, 1);
        assert!(plan.metadata.feasibility.target_partition_size_satisfied);
        assert_eq!(plan.metadata.feasibility.reason, None);
    }

    #[test]
    fn uniform_dataset_without_skew_produces_no_op_plan() {
        let config = config_with_rewrite_controls(false, 1.2);
        let dataset = input_dataset_with_file_size(
            Dataset::from_key_values(
                "user_id",
                (0..100).flat_map(|_| ["a", "b", "c", "d"].into_iter().map(String::from)),
            ),
            1024,
        );
        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");

        let plan = build_plan(&config, &statistics).expect("plan should build");

        assert!(statistics.metadata.skew.max_mean_imbalance_ratio <= 1.2);
        assert!(statistics.metadata.input.heavy_hitters.is_empty());
        assert!(plan.metadata.heavy_keys.is_empty());
        assert_eq!(plan.metadata.action, PlanAction::NoOp);
        assert!(!plan.metadata.rewrite_required);
        assert_eq!(
            plan.metadata.skip_reason.as_deref(),
            Some("no_rewrite_needed")
        );
        assert_eq!(plan.metadata.cost_estimate.estimated_rows_written, 0);
        assert_eq!(plan.metadata.cost_estimate.estimated_bytes_written, Some(0));
    }

    #[test]
    fn heavy_key_dataset_produces_rewrite_plan() {
        let config = example_config();
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            std::iter::repeat_n("heavy", 40).chain(["a", "b", "c", "d"]),
        ));
        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");

        let plan = build_plan(&config, &statistics).expect("plan should build");

        assert_eq!(plan.metadata.action, PlanAction::Rewrite);
        assert!(plan.metadata.rewrite_required);
        assert_eq!(plan.metadata.cost_estimate.reason, "heavy_keys_detected");
        assert_eq!(plan.metadata.cost_estimate.estimated_rows_written, 44);
    }

    #[test]
    fn force_rewrite_overrides_no_op_decision() {
        let config = config_with_rewrite_controls(true, 1.2);
        let dataset = input_dataset_with_file_size(
            Dataset::from_key_values(
                "user_id",
                ["a", "a", "b", "b", "c", "c", "d", "d"]
                    .into_iter()
                    .map(String::from),
            ),
            1024,
        );
        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");

        let plan = build_plan(&config, &statistics).expect("plan should build");

        assert_eq!(plan.metadata.action, PlanAction::Rewrite);
        assert!(plan.metadata.rewrite_required);
        assert_eq!(plan.metadata.cost_estimate.reason, "force_rewrite");
        assert_eq!(plan.metadata.cost_estimate.estimated_rows_written, 8);
    }

    #[test]
    fn large_dataset_is_capped_by_max_partitions() {
        let config = config_with_partition_limits(1, 4, 1);
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            (0..10).map(|index| format!("key_{index}")),
        ));
        let mut statistics =
            compute_statistics(&config, &dataset).expect("statistics should compute");
        statistics.metadata.input.estimated_row_width_bytes = Some(1024 * 1024);

        let plan = build_plan(&config, &statistics).expect("plan should build");

        assert_eq!(plan.metadata.required_partitions_by_size, 10);
        assert_eq!(plan.metadata.output_partitions, 4);
        assert!(!plan.metadata.feasibility.target_partition_size_satisfied);
        assert_eq!(
            plan.metadata.feasibility.reason.as_deref(),
            Some(targeting::REASON_REQUIRED_PARTITIONS_EXCEED_MAX)
        );
    }

    #[test]
    fn plan_salts_keys_that_exceed_target_rows_even_without_mean_outliers() {
        let config = config_with_partition_limits(1, 128, 1);
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

        let plan = build_plan(&config, &statistics).expect("plan should build");

        assert!(statistics.metadata.input.heavy_hitter_candidates.is_empty());
        assert_eq!(statistics.metadata.input.heavy_hitters.len(), 4);
        assert_eq!(plan.metadata.target_partition_rows, 100);
        assert_eq!(plan.metadata.heavy_keys.len(), 4);
        assert!(plan.metadata.normal_keys.is_empty());
        assert!(plan
            .metadata
            .heavy_keys
            .iter()
            .all(|heavy| heavy.salt_count == 25 && !heavy.salt_partitions.is_empty()));
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
        let config = config_with_normal_key_assignment("hash");
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
    fn load_aware_assignment_reduces_normal_key_hash_skew() {
        let config = config_with_normal_key_assignment("load_aware");
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            normal_key_values_for_hash_partition(0, 12),
        ));
        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");
        let plan = build_plan(&config, &statistics).expect("plan should build");
        let planned_sizes = planned_normal_partition_sizes(&plan);

        assert!(plan.metadata.heavy_keys.is_empty());
        assert_eq!(
            plan.metadata.normal_key_assignment,
            NormalKeyAssignment::LoadAware
        );
        assert!(
            max_mean_imbalance_ratio(&planned_sizes)
                < max_mean_imbalance_ratio(&statistics.metadata.estimates.before_partition_sizes)
        );
    }

    #[test]
    fn hash_assignment_preserves_previous_behavior_when_configured() {
        let config = config_with_normal_key_assignment("hash");
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            normal_key_values_for_hash_partition(0, 12),
        ));
        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");
        let plan = build_plan(&config, &statistics).expect("plan should build");

        assert_eq!(
            plan.metadata.normal_key_assignment,
            NormalKeyAssignment::Hash
        );
        assert!(plan.metadata.normal_keys.iter().all(|normal| {
            normal.partition_id
                == hashing::partition_id(
                    &normal.key,
                    plan.metadata.output_partitions,
                    plan.metadata.seed,
                )
        }));
        assert_eq!(plan.metadata.action, PlanAction::NoOp);
        assert_eq!(
            plan.metadata.skip_reason.as_deref(),
            Some("planned_distribution_does_not_improve_skew")
        );
    }

    #[test]
    fn rewrite_with_normal_key_skew_improves_after_partition_sizes() {
        let config = config_with_normal_key_assignment("load_aware");
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            normal_key_values_for_hash_partition(0, 12),
        ));
        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");
        let plan = build_plan(&config, &statistics).expect("plan should build");
        let planned_sizes = planned_normal_partition_sizes(&plan);

        assert_eq!(plan.metadata.action, PlanAction::Rewrite);
        assert!(plan.metadata.rewrite_required);
        assert_eq!(
            plan.metadata.cost_estimate.reason,
            "max_partition_exceeds_target_rows"
        );
        assert!(
            max_mean_imbalance_ratio(&planned_sizes)
                < max_mean_imbalance_ratio(&statistics.metadata.estimates.before_partition_sizes)
        );
    }

    #[test]
    fn normal_key_assignment_is_deterministic() {
        let config = config_with_normal_key_assignment("load_aware");
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            normal_key_values_for_hash_partition(0, 12),
        ));
        let statistics = compute_statistics(&config, &dataset).expect("statistics should compute");

        let first_plan = build_plan(&config, &statistics).expect("first plan should build");
        let second_plan = build_plan(&config, &statistics).expect("second plan should build");

        assert_eq!(
            first_plan.metadata.normal_keys,
            second_plan.metadata.normal_keys
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
        assert!(json.contains("\"required_partitions_by_size\""));
        assert!(json.contains("\"feasibility\""));
        assert!(json.contains("\"recommended_downstream_plan\""));
        assert!(json.contains("\"two_stage_group_by\""));
        assert!(json.contains("\"normal_key_assignment\":\"load_aware\""));
    }

    #[test]
    fn group_by_plan_recommends_two_stage_group_by() {
        let config = config_with_job_type("group_by");
        let plan = build_plan_for_job_config(&config);

        let recommendation = &plan.metadata.recommended_downstream_plan;
        assert_eq!(plan.metadata.job_type, crate::config::JobType::GroupBy);
        assert_eq!(recommendation.strategy, "two_stage_group_by");
        assert!(recommendation.requires_operator_rewrite);
        assert_eq!(
            recommendation.partition_column.as_deref(),
            Some("_rp_partition_id")
        );
        assert_eq!(recommendation.salt_column.as_deref(), Some("_rp_salt"));
        assert_eq!(
            recommendation.heavy_key_column.as_deref(),
            Some("_rp_is_heavy_key")
        );
        assert_eq!(
            recommendation.partial_group_keys,
            vec![
                "_rp_partition_id".to_string(),
                "_rp_salt".to_string(),
                "user_id".to_string()
            ]
        );
        assert_eq!(recommendation.final_group_keys, vec!["user_id".to_string()]);
    }

    #[test]
    fn group_by_recommendation_contains_salt_column() {
        let config = config_with_job_type("group_by");
        let plan = build_plan_for_job_config(&config);

        assert!(plan
            .metadata
            .recommended_downstream_plan
            .partial_group_keys
            .contains(&"_rp_salt".to_string()));
    }

    #[test]
    fn group_by_plan_notes_missing_technical_columns_for_method_aware_mode() {
        let config = config_with_disabled_technical_columns("group_by");
        let plan = build_plan_for_job_config(&config);
        let recommendation = &plan.metadata.recommended_downstream_plan;

        assert_eq!(
            recommendation.partial_group_keys,
            vec!["user_id".to_string()]
        );
        assert_eq!(recommendation.salt_column, None);
        assert!(recommendation
            .notes
            .contains(&"method_aware_group_by_requires_technical_columns".to_string()));
    }

    #[test]
    fn join_recommendation_records_missing_join_plan_note() {
        let config = config_with_job_type("join");
        let plan = build_plan_for_job_config(&config);

        let recommendation = &plan.metadata.recommended_downstream_plan;
        assert_eq!(plan.metadata.job_type, crate::config::JobType::Join);
        assert!(plan.metadata.join_plan.is_none());
        assert_eq!(recommendation.strategy, "generic_join_repartitioning");
        assert!(recommendation.requires_operator_rewrite);
        assert_eq!(recommendation.join_keys, vec!["user_id".to_string()]);
        assert!(recommendation
            .notes
            .contains(&"join_plan_missing".to_string()));
        assert_eq!(recommendation.salt_column.as_deref(), Some("_rp_salt"));
        assert_eq!(
            recommendation.heavy_key_column.as_deref(),
            Some("_rp_is_heavy_key")
        );
    }

    #[test]
    fn join_recommendation_uses_broadcast_strategy_for_small_right_side() {
        let config = config_with_join(10);
        let left_dataset =
            InputDataset::from_rows(Dataset::from_key_values("user_id", ["a", "b", "c", "d"]));
        let right_dataset = input_dataset_with_file_size(
            Dataset::from_key_values("user_id", ["a", "b"]),
            2 * 1024 * 1024,
        );

        let plan = build_join_plan(&config, left_dataset, right_dataset);
        let join_plan = plan.metadata.join_plan.as_ref().unwrap();
        let recommendation = &plan.metadata.recommended_downstream_plan;

        assert_eq!(
            join_plan.recommended_strategy,
            "broadcast_join_recommendation"
        );
        assert_eq!(recommendation.strategy, "broadcast_join");
        assert!(recommendation.requires_operator_rewrite);
        assert!(recommendation
            .notes
            .contains(&"right_side_broadcastable".to_string()));
        assert_eq!(join_plan.right_side_size_mb, Some(2));
        assert_eq!(join_plan.broadcast_threshold_mb, Some(10));
    }

    #[test]
    fn join_recommendation_uses_salted_strategy_for_shared_heavy_keys() {
        let config = config_with_join(0);
        let left_dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            std::iter::repeat_n("heavy", 40).chain(["a", "b", "c", "d"]),
        ));
        let right_dataset = input_dataset_with_file_size(
            Dataset::from_key_values(
                "user_id",
                std::iter::repeat_n("heavy", 30).chain(["x", "y"]),
            ),
            2 * 1024 * 1024,
        );

        let plan = build_join_plan(&config, left_dataset, right_dataset);
        let join_plan = plan.metadata.join_plan.as_ref().unwrap();
        let recommendation = &plan.metadata.recommended_downstream_plan;

        assert_eq!(join_plan.recommended_strategy, "salted_heavy_key_join");
        assert_eq!(recommendation.strategy, "salted_heavy_key_join");
        assert!(recommendation.requires_operator_rewrite);
        assert!(recommendation
            .notes
            .contains(&"shared_heavy_keys_detected".to_string()));
        assert_eq!(
            join_plan.shared_heavy_keys,
            vec!["7:user_id#utf8:5:heavy".to_string()]
        );
        assert_eq!(join_plan.shared_heavy_key_values.len(), 1);
        assert_eq!(
            join_plan.shared_heavy_key_values[0].encoded,
            "7:user_id#utf8:5:heavy"
        );
        assert_eq!(
            join_plan.shared_heavy_key_values[0].parts[0].column,
            "user_id"
        );
        assert_eq!(
            join_plan.shared_heavy_key_values[0].parts[0].value_type,
            "utf8"
        );
        assert_eq!(
            join_plan.shared_heavy_key_values[0].parts[0]
                .value
                .as_deref(),
            Some("heavy")
        );
        assert_eq!(join_plan.left_heavy_keys.len(), 1);
        assert_eq!(join_plan.right_heavy_keys.len(), 1);
    }

    #[test]
    fn join_plan_contains_shared_structured_heavy_keys() {
        let config = config_with_join(0);
        let left_dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            std::iter::repeat_n("heavy", 40).chain(["a", "b", "c", "d"]),
        ));
        let right_dataset = input_dataset_with_file_size(
            Dataset::from_key_values(
                "user_id",
                std::iter::repeat_n("heavy", 30).chain(["x", "y"]),
            ),
            2 * 1024 * 1024,
        );

        let plan = build_join_plan(&config, left_dataset, right_dataset);
        let join_plan = plan.metadata.join_plan.as_ref().unwrap();

        assert_eq!(join_plan.left_heavy_key_values.len(), 1);
        assert_eq!(join_plan.right_heavy_key_values.len(), 1);
        assert_eq!(join_plan.shared_heavy_key_values.len(), 1);
        assert_eq!(
            join_plan.shared_heavy_key_values[0].parts[0]
                .value
                .as_deref(),
            Some("heavy")
        );
    }

    #[test]
    fn join_recommendation_uses_heavy_key_isolation_for_one_sided_heavy_keys() {
        let config = config_with_join(0);
        let left_dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            std::iter::repeat_n("heavy", 40).chain(["a", "b", "c", "d"]),
        ));
        let right_dataset = input_dataset_with_file_size(
            Dataset::from_key_values("user_id", (0..40).map(|index| format!("right_{index}"))),
            2 * 1024 * 1024,
        );

        let plan = build_join_plan(&config, left_dataset, right_dataset);
        let join_plan = plan.metadata.join_plan.as_ref().unwrap();
        let recommendation = &plan.metadata.recommended_downstream_plan;

        assert_eq!(join_plan.recommended_strategy, "heavy_key_isolation_join");
        assert_eq!(recommendation.strategy, "heavy_key_isolation_join");
        assert!(recommendation.requires_operator_rewrite);
        assert!(recommendation
            .notes
            .contains(&"one_sided_heavy_keys_detected".to_string()));
        assert!(join_plan.shared_heavy_keys.is_empty());
        assert_eq!(join_plan.left_heavy_keys.len(), 1);
        assert!(join_plan.right_heavy_keys.is_empty());
    }

    #[test]
    fn join_recommendation_uses_physical_only_without_heavy_keys() {
        let config = config_with_join(0);
        let left_dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            (0..40).map(|index| format!("left_{index}")),
        ));
        let right_dataset = input_dataset_with_file_size(
            Dataset::from_key_values("user_id", (0..40).map(|index| format!("right_{index}"))),
            2 * 1024 * 1024,
        );

        let plan = build_join_plan(&config, left_dataset, right_dataset);
        let join_plan = plan.metadata.join_plan.as_ref().unwrap();
        let recommendation = &plan.metadata.recommended_downstream_plan;

        assert_eq!(join_plan.recommended_strategy, "physical_only");
        assert_eq!(recommendation.strategy, "physical_repartitioning");
        assert!(!recommendation.requires_operator_rewrite);
        assert!(recommendation
            .notes
            .contains(&"no_join_heavy_keys_detected".to_string()));
        assert!(join_plan.left_heavy_keys.is_empty());
        assert!(join_plan.right_heavy_keys.is_empty());
        assert!(join_plan.shared_heavy_keys.is_empty());
    }

    #[test]
    fn scan_plan_does_not_require_operator_rewrite() {
        let config = config_with_job_type("scan");
        let plan = build_plan_for_job_config(&config);

        let recommendation = &plan.metadata.recommended_downstream_plan;
        assert_eq!(plan.metadata.job_type, crate::config::JobType::Scan);
        assert_eq!(recommendation.strategy, "physical_repartitioning");
        assert!(!recommendation.requires_operator_rewrite);
        assert_eq!(
            recommendation.partition_column.as_deref(),
            Some("_rp_partition_id")
        );
    }

    #[test]
    fn filter_plan_records_unknown_selectivity_note() {
        let config = config_with_job_type("filter");
        let plan = build_plan_for_job_config(&config);

        let recommendation = &plan.metadata.recommended_downstream_plan;
        assert_eq!(
            recommendation.strategy,
            "physical_repartitioning_with_filter_awareness"
        );
        assert!(!recommendation.requires_operator_rewrite);
        assert!(recommendation
            .notes
            .contains(&"filter_selectivity_unknown".to_string()));
    }

    fn config_with_partition_limits(
        min_partitions: usize,
        max_partitions: usize,
        target_partition_size_mb: u64,
    ) -> Config {
        Config::from_yaml_str(&format!(
            r#"
dataset:
  input: "./data/input.parquet"
  output: "./data/output_partitioned"
  format: "parquet"

partitioning:
  key_columns: ["user_id"]
  min_partitions: {min_partitions}
  target_partition_size_mb: {target_partition_size_mb}
  max_partitions: {max_partitions}
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

    fn config_with_rewrite_controls(force_rewrite: bool, no_op_max_imbalance_ratio: f64) -> Config {
        Config::from_yaml_str(&format!(
            r#"
dataset:
  input: "./data/input.parquet"
  output: "./data/output_partitioned"
  format: "parquet"

partitioning:
  key_columns: ["user_id"]
  target_partition_size_mb: 128
  max_partitions: 128
  strategy: "adaptive_hash_salt"
  heavy_key_alpha: 2.0
  force_rewrite: {force_rewrite}
  no_op_max_imbalance_ratio: {no_op_max_imbalance_ratio}
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

    fn config_with_normal_key_assignment(normal_key_assignment: &str) -> Config {
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
  normal_key_assignment: "{normal_key_assignment}"
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

    fn config_with_job_type(job_type: &str) -> Config {
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
  type: "{job_type}"
  downstream_engine: "spark"

resources:
  local_threads: 8
  memory_limit_mb: 4096
"#
        ))
        .expect("test config should parse")
    }

    fn config_with_disabled_technical_columns(job_type: &str) -> Config {
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

output:
  include_technical_columns: false

job:
  type: "{job_type}"
  downstream_engine: "spark"

resources:
  local_threads: 8
  memory_limit_mb: 4096
"#
        ))
        .expect("test config should parse")
    }

    fn config_with_join(broadcast_threshold_mb: u64) -> Config {
        Config::from_yaml_str(&format!(
            r#"
dataset:
  input: "./data/left.parquet"
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
  type: "join"
  downstream_engine: "spark"

join:
  left_input: "./data/left.parquet"
  right_input: "./data/right.parquet"
  join_keys: ["user_id"]
  right_side_mode: "broadcast_if_small"
  broadcast_threshold_mb: {broadcast_threshold_mb}

resources:
  local_threads: 8
  memory_limit_mb: 4096
"#
        ))
        .expect("join test config should parse")
    }

    fn build_plan_for_job_config(config: &Config) -> Plan {
        let dataset = InputDataset::from_rows(Dataset::from_key_values(
            "user_id",
            ["heavy", "heavy", "heavy", "heavy", "a", "b", "c", "d"],
        ));
        let statistics = compute_statistics(config, &dataset).expect("statistics should compute");
        build_plan(config, &statistics).expect("plan should build")
    }

    fn build_join_plan(
        config: &Config,
        left_dataset: InputDataset,
        right_dataset: InputDataset,
    ) -> Plan {
        let mut left_statistics =
            compute_statistics(config, &left_dataset).expect("left statistics should compute");
        let right_statistics =
            compute_statistics(config, &right_dataset).expect("right statistics should compute");
        left_statistics.set_join_statistics(build_join_statistics(
            config.join.as_ref().unwrap().join_keys.clone(),
            &left_statistics,
            Some(&right_statistics),
        ));

        build_plan(config, &left_statistics).expect("join plan should build")
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

    fn planned_normal_partition_sizes(plan: &Plan) -> Vec<u64> {
        let mut sizes = vec![0; plan.metadata.output_partitions];
        for normal in &plan.metadata.normal_keys {
            sizes[normal.partition_id] += normal.estimated_frequency;
        }
        sizes
    }
}
