use crate::Config;

pub const REASON_ESTIMATED_ROW_WIDTH_UNAVAILABLE: &str = "estimated_row_width_unavailable";
pub const REASON_REQUIRED_PARTITIONS_EXCEED_MAX: &str = "required_partitions_exceed_max_partitions";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetPartitioning {
    pub output_partitions: usize,
    pub target_partition_rows: u64,
    pub required_partitions_by_size: usize,
    pub target_size_satisfied: bool,
    pub reason: Option<String>,
}

pub fn compute_target_partitioning(
    config: &Config,
    total_rows: u64,
    estimated_row_width_bytes: Option<u64>,
) -> TargetPartitioning {
    let min_partitions = config.partitioning.min_partitions.get();
    let max_partitions = config.partitioning.max_partitions.get();

    let Some(row_width_bytes) = estimated_row_width_bytes else {
        let output_partitions = max_partitions;
        return TargetPartitioning {
            output_partitions,
            target_partition_rows: rows_per_partition(total_rows, output_partitions),
            required_partitions_by_size: output_partitions,
            target_size_satisfied: false,
            reason: Some(REASON_ESTIMATED_ROW_WIDTH_UNAVAILABLE.to_string()),
        };
    };

    let target_size_bytes = config
        .partitioning
        .target_partition_size_mb
        .get()
        .saturating_mul(1024 * 1024);
    let total_size_bytes = total_rows.saturating_mul(row_width_bytes);
    let required_partitions_by_size =
        usize_from_u64(total_size_bytes.div_ceil(target_size_bytes).max(1));
    let output_partitions = required_partitions_by_size.clamp(min_partitions, max_partitions);
    let rows_by_output_count = rows_per_partition(total_rows, output_partitions);
    let rows_by_target_size = (target_size_bytes / row_width_bytes.max(1)).max(1);
    let target_size_satisfied = required_partitions_by_size <= max_partitions;

    TargetPartitioning {
        output_partitions,
        target_partition_rows: rows_by_output_count.min(rows_by_target_size).max(1),
        required_partitions_by_size,
        target_size_satisfied,
        reason: if target_size_satisfied {
            None
        } else {
            Some(REASON_REQUIRED_PARTITIONS_EXCEED_MAX.to_string())
        },
    }
}

fn rows_per_partition(total_rows: u64, output_partitions: usize) -> u64 {
    if total_rows == 0 || output_partitions == 0 {
        return 1;
    }

    total_rows.div_ceil(output_partitions as u64).max(1)
}

fn usize_from_u64(value: u64) -> usize {
    value.min(usize::MAX as u64) as usize
}
