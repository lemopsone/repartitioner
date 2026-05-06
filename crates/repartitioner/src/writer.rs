use std::{fs, fs::File, path::Path, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::take::take;
use parquet::arrow::ArrowWriter;

use crate::{
    manifest::{
        write_json_metadata, Manifest, OutputFile, PartitionManifest, PartitionPlan, StatsMetadata,
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
    let output_dir = output_dir.as_ref();
    fs::create_dir_all(output_dir).map_err(|source| Error::WriteFile {
        path: output_dir.to_path_buf(),
        source,
    })?;

    let mut rows_by_partition = vec![Vec::new(); plan.output_partitions];
    for record in &assignments.records {
        if record.partition_id < rows_by_partition.len() {
            rows_by_partition[record.partition_id].push(record.row_index);
        }
    }

    let mut output_files = Vec::new();
    for (partition_id, row_indexes) in rows_by_partition.iter().enumerate() {
        if row_indexes.is_empty() {
            continue;
        }

        let relative_path = format!("ap_partition={partition_id}/part-{partition_id:05}.parquet");
        let file_path = output_dir.join(&relative_path);
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).map_err(|source| Error::WriteFile {
                path: parent.to_path_buf(),
                source,
            })?;
        }

        write_partition_parquet(&file_path, plan, dataset, row_indexes)?;
        let size_bytes = fs::metadata(&file_path).ok().map(|metadata| metadata.len());
        output_files.push(OutputFile {
            path: relative_path,
            partition_id,
            row_count: row_indexes.len() as u64,
            size_bytes,
        });
    }

    let partitions = assignments
        .partition_row_counts
        .iter()
        .enumerate()
        .map(|(partition_id, row_count)| {
            let partition_files: Vec<_> = output_files
                .iter()
                .filter(|file| file.partition_id == partition_id)
                .collect();
            let size_bytes = partition_files
                .iter()
                .filter_map(|file| file.size_bytes)
                .reduce(|left, right| left + right);

            PartitionManifest {
                partition_id,
                row_count: *row_count,
                file_count: partition_files.len(),
                size_bytes,
            }
        })
        .collect();

    let manifest = Manifest {
        version: METADATA_VERSION.to_string(),
        input_reused: false,
        dataset_location: None,
        output_files,
        partitions,
    };

    write_metadata_files(output_dir, plan, stats, &manifest)?;

    Ok(WriteSummary { manifest })
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

fn write_metadata_files(
    output_dir: &Path,
    plan: &PartitionPlan,
    stats: &StatsMetadata,
    manifest: &Manifest,
) -> Result<()> {
    write_json_metadata(output_dir.join("_partition_plan.json"), plan)?;
    write_json_metadata(output_dir.join("_stats.json"), stats)?;
    write_json_metadata(output_dir.join("_manifest.json"), manifest)
}

fn write_partition_parquet(
    file_path: &Path,
    plan: &PartitionPlan,
    dataset: &InputDataset,
    row_indexes: &[usize],
) -> Result<()> {
    if !dataset.batches.is_empty() {
        return write_retained_rows_parquet(file_path, dataset, row_indexes);
    }

    let schema = Arc::new(Schema::new(
        plan.key_columns
            .iter()
            .map(|column| Field::new(column, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));
    let columns = plan
        .key_columns
        .iter()
        .map(|column| {
            let values = row_indexes
                .iter()
                .map(|row_index| {
                    dataset
                        .rows
                        .rows
                        .get(*row_index)
                        .and_then(|row| row.key_values().get(column))
                        .cloned()
                })
                .collect::<Vec<_>>();
            Arc::new(StringArray::from(values)) as ArrayRef
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    let file = File::create(file_path).map_err(|source| Error::WriteFile {
        path: file_path.to_path_buf(),
        source,
    })?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

fn write_retained_rows_parquet(
    file_path: &Path,
    dataset: &InputDataset,
    row_indexes: &[usize],
) -> Result<()> {
    let schema = dataset.batches[0].schema();
    let file = File::create(file_path).map_err(|source| Error::WriteFile {
        path: file_path.to_path_buf(),
        source,
    })?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None)?;

    for batch in retained_partition_batches(dataset, row_indexes)? {
        writer.write(&batch)?;
    }
    writer.close()?;

    Ok(())
}

fn retained_partition_batches(
    dataset: &InputDataset,
    row_indexes: &[usize],
) -> Result<Vec<RecordBatch>> {
    let mut result = Vec::new();
    let mut batch_start = 0_usize;
    let mut row_cursor = 0_usize;

    for batch in &dataset.batches {
        let batch_end = batch_start + batch.num_rows();
        if row_cursor < row_indexes.len() && row_indexes[row_cursor] < batch_start {
            return Err(Error::MissingRetainedRow {
                row_index: row_indexes[row_cursor],
            });
        }

        let mut local_indexes = Vec::new();
        while row_cursor < row_indexes.len() && row_indexes[row_cursor] < batch_end {
            local_indexes.push((row_indexes[row_cursor] - batch_start) as u32);
            row_cursor += 1;
        }

        if !local_indexes.is_empty() {
            result.push(take_batch(batch, &local_indexes)?);
        }

        batch_start = batch_end;
    }

    if let Some(row_index) = row_indexes.get(row_cursor) {
        return Err(Error::MissingRetainedRow {
            row_index: *row_index,
        });
    }

    Ok(result)
}

fn take_batch(batch: &RecordBatch, local_indexes: &[u32]) -> Result<RecordBatch> {
    let indexes = UInt32Array::from(local_indexes.to_vec());
    let columns = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indexes, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}
