use crate::{planner::Plan, statistics::ComputedStatistics, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionAssignmentSummary {
    pub partition_row_counts: Vec<u64>,
}

pub fn assign_partitions(
    plan: &Plan,
    _statistics: &ComputedStatistics,
) -> Result<PartitionAssignmentSummary> {
    Ok(PartitionAssignmentSummary {
        partition_row_counts: vec![0; plan.metadata.output_partitions],
    })
}
