use std::{fs, path::Path};

use crate::{
    config::DatasetFormat,
    io::{parquet::ParquetDatasetWriter, DatasetWriter},
    manifest::{
        write_json_metadata, Manifest, PartitionManifest, PartitionPlan, StatsMetadata,
        METADATA_VERSION,
    },
    partitioner::PartitionAssignmentSummary,
    reader::InputDataset,
    Error, Result,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteSummary {
    pub manifest: Manifest,
}

pub fn write_output(
    output_dir: impl AsRef<Path>,
    plan: &PartitionPlan,
    stats: &StatsMetadata,
    assignments: &PartitionAssignmentSummary,
    dataset: &InputDataset,
) -> Result<WriteSummary> {
    match &dataset.format {
        DatasetFormat::Parquet => ParquetDatasetWriter.write_output(
            output_dir.as_ref(),
            plan,
            stats,
            assignments,
            dataset,
        ),
    }
}

pub fn write_no_op_output(
    output_dir: impl AsRef<Path>,
    plan: &PartitionPlan,
    stats: &StatsMetadata,
    dataset: &InputDataset,
) -> Result<WriteSummary> {
    let output_dir = output_dir.as_ref();
    fs::create_dir_all(output_dir).map_err(|source| Error::WriteFile {
        path: output_dir.to_path_buf(),
        source,
    })?;

    let partitions = stats
        .estimates
        .before_partition_sizes
        .iter()
        .enumerate()
        .map(|(partition_id, row_count)| PartitionManifest {
            partition_id,
            row_count: *row_count,
            file_count: 0,
            size_bytes: None,
        })
        .collect();
    let manifest = Manifest {
        version: METADATA_VERSION.to_string(),
        input_reused: true,
        dataset_location: Some(dataset.path.clone()),
        output_files: Vec::new(),
        partitions,
    };

    write_metadata_files(output_dir, plan, stats, &manifest)?;

    Ok(WriteSummary { manifest })
}

pub(crate) fn write_metadata_files(
    output_dir: &Path,
    plan: &PartitionPlan,
    stats: &StatsMetadata,
    manifest: &Manifest,
) -> Result<()> {
    write_json_metadata(output_dir.join("_partition_plan.json"), plan)?;
    write_json_metadata(output_dir.join("_stats.json"), stats)?;
    write_json_metadata(output_dir.join("_manifest.json"), manifest)
}
