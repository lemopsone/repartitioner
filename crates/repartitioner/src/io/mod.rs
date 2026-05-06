use std::path::Path;

use arrow_array::RecordBatch;

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

pub trait KeyBatchScanner {
    fn scan_key_batches(&self, config: &Config) -> Result<InputDataset>;
}

pub trait RecordBatchScanner {
    fn scan_record_batches(
        &self,
        dataset: &InputDataset,
        visitor: &mut dyn FnMut(usize, RecordBatch) -> Result<()>,
    ) -> Result<()>;
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
