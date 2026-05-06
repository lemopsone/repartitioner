use std::path::Path;

use crate::{
    manifest::{PartitionPlan, StatsMetadata},
    partitioner::PartitionAssignmentSummary,
    reader::InputDataset,
    writer::WriteSummary,
    Config, Result,
};

pub mod csv;
pub mod parquet;

pub trait DatasetReader {
    fn read_dataset(&self, config: &Config) -> Result<InputDataset>;
}

pub trait DatasetWriter {
    fn write_output(
        &self,
        output_dir: &Path,
        plan: &PartitionPlan,
        stats: &StatsMetadata,
        assignments: &PartitionAssignmentSummary,
        dataset: &InputDataset,
    ) -> Result<WriteSummary>;
}
