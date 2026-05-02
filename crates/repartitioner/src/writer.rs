use std::{fs, path::Path};

use crate::{
    manifest::{
        write_json_metadata, Manifest, OutputFile, PartitionManifest, PartitionPlan, StatsMetadata,
        METADATA_VERSION,
    },
    partitioner::PartitionAssignmentSummary,
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
) -> Result<WriteSummary> {
    let output_dir = output_dir.as_ref();
    fs::create_dir_all(output_dir).map_err(|source| Error::WriteFile {
        path: output_dir.to_path_buf(),
        source,
    })?;

    let partitions = assignments
        .partition_row_counts
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
        output_files: Vec::<OutputFile>::new(),
        partitions,
    };

    write_json_metadata(output_dir.join("_partition_plan.json"), plan)?;
    write_json_metadata(output_dir.join("_stats.json"), stats)?;
    write_json_metadata(output_dir.join("_manifest.json"), &manifest)?;

    Ok(WriteSummary { manifest })
}
