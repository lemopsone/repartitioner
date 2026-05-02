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
    let heavy_keys = statistics
        .metadata
        .input
        .heavy_hitters
        .iter()
        .map(|heavy| HeavyKeyPlan {
            key: heavy.key.clone(),
            estimated_frequency: heavy.estimated_frequency,
            salt_count: heavy.salt_count,
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

fn creation_timestamp() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();

    format!("unix_seconds:{seconds}")
}
