use std::{collections::BTreeMap, fs, fs::File, path::Path, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Decimal256Array, Int16Array,
    Int32Array, Int64Array, Int8Array, LargeStringArray, RecordBatch, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use arrow_select::take::take;
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};

use crate::{
    config::DatasetFormat,
    dataset::{Dataset, Row},
    io::{DatasetReader, DatasetWriter},
    key_encoding::{key_value_to_string, KeyValue},
    manifest::{
        Manifest, OutputFile, PartitionManifest, PartitionPlan, StatsMetadata, TechnicalColumns,
        METADATA_VERSION,
    },
    partitioner::{PartitionAssignmentSummary, RecordPartitionAssignment},
    reader::{InputDataset, InputFile},
    writer::{write_metadata_files, WriteSummary},
    Config, Error, Result,
};

#[derive(Debug, Clone, Copy, Default)]
pub struct ParquetDatasetReader;

impl DatasetReader for ParquetDatasetReader {
    fn read_dataset(&self, config: &Config) -> Result<InputDataset> {
        inspect_local_input(
            &config.dataset.input,
            DatasetFormat::Parquet,
            &config.partitioning.key_columns,
        )
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ParquetDatasetWriter;

impl DatasetWriter for ParquetDatasetWriter {
    fn write_output(
        &self,
        output_dir: &Path,
        plan: &PartitionPlan,
        stats: &StatsMetadata,
        assignments: &PartitionAssignmentSummary,
        dataset: &InputDataset,
    ) -> Result<WriteSummary> {
        write_parquet_output(output_dir, plan, stats, assignments, dataset)
    }
}

fn inspect_local_input(
    input: &Path,
    format: DatasetFormat,
    key_columns: &[String],
) -> Result<InputDataset> {
    let files = if input.is_file() {
        vec![inspect_file(input)?]
    } else if input.is_dir() {
        let mut files = Vec::new();
        collect_parquet_files(input, &mut files)?;
        files.sort_by(|left, right| left.path.cmp(&right.path));
        files
    } else {
        return Err(Error::InputPathNotFound {
            path: input.to_path_buf(),
        });
    };
    if files.is_empty() {
        return Err(Error::NoParquetFiles {
            path: input.to_path_buf(),
        });
    }

    let (rows, batches) = read_parquet_dataset(&files, key_columns)?;

    Ok(InputDataset {
        path: input.display().to_string(),
        format,
        files,
        rows,
        batches,
    })
}

fn collect_parquet_files(dir: &Path, files: &mut Vec<InputFile>) -> Result<()> {
    for entry in fs::read_dir(dir).map_err(|source| Error::ReadFile {
        path: dir.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| Error::ReadFile {
            path: dir.to_path_buf(),
            source,
        })?;
        let entry_path = entry.path();
        if entry_path.is_dir() {
            collect_parquet_files(&entry_path, files)?;
        } else if entry_path.is_file()
            && entry_path
                .extension()
                .is_some_and(|ext| ext == std::ffi::OsStr::new("parquet"))
        {
            files.push(inspect_file(&entry_path)?);
        }
    }

    Ok(())
}

fn inspect_file(path: &Path) -> Result<InputFile> {
    let metadata = fs::metadata(path).map_err(|source| Error::ReadFile {
        path: path.to_path_buf(),
        source,
    })?;

    Ok(InputFile {
        path: path.display().to_string(),
        size_bytes: metadata.len(),
    })
}

fn read_parquet_dataset(
    files: &[InputFile],
    key_columns: &[String],
) -> Result<(Dataset, Vec<RecordBatch>)> {
    let mut rows = Vec::new();
    let mut batches = Vec::new();

    for input_file in files {
        let file = File::open(&input_file.path).map_err(|source| Error::ReadFile {
            path: input_file.path.clone().into(),
            source,
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        for batch in reader {
            let batch = batch?;
            let schema = batch.schema();
            let column_indexes = key_columns
                .iter()
                .map(|column| schema.index_of(column))
                .collect::<std::result::Result<Vec<_>, _>>()?;

            for row_index in 0..batch.num_rows() {
                let mut key_values = BTreeMap::new();
                for (column, column_index) in key_columns.iter().zip(column_indexes.iter()) {
                    let array = batch.column(*column_index);
                    let value = key_value(column, array.as_ref(), row_index)?;
                    key_values.insert(column.clone(), value);
                }
                rows.push(Row::new(key_values));
            }
            batches.push(batch);
        }
    }

    Ok((Dataset::new(rows), batches))
}

fn key_value(column: &str, array: &dyn Array, row_index: usize) -> Result<KeyValue> {
    if array.is_null(row_index) {
        return Ok(KeyValue::Null);
    }

    match array.data_type() {
        DataType::Utf8 => Ok(array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 array should downcast to StringArray")
            .value(row_index)
            .to_string()
            .into()),
        DataType::LargeUtf8 => Ok(array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .expect("LargeUtf8 array should downcast to LargeStringArray")
            .value(row_index)
            .to_string()
            .into()),
        DataType::Int8 => Ok(KeyValue::Int64(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("Int8 array should downcast to Int8Array")
                .value(row_index) as i64,
        )),
        DataType::Int16 => Ok(KeyValue::Int64(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .expect("Int16 array should downcast to Int16Array")
                .value(row_index) as i64,
        )),
        DataType::Int32 => Ok(KeyValue::Int64(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32 array should downcast to Int32Array")
                .value(row_index) as i64,
        )),
        DataType::Int64 => Ok(KeyValue::Int64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 array should downcast to Int64Array")
                .value(row_index),
        )),
        DataType::UInt8 => Ok(KeyValue::UInt64(
            array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .expect("UInt8 array should downcast to UInt8Array")
                .value(row_index) as u64,
        )),
        DataType::UInt16 => Ok(KeyValue::UInt64(
            array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .expect("UInt16 array should downcast to UInt16Array")
                .value(row_index) as u64,
        )),
        DataType::UInt32 => Ok(KeyValue::UInt64(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .expect("UInt32 array should downcast to UInt32Array")
                .value(row_index) as u64,
        )),
        DataType::UInt64 => Ok(KeyValue::UInt64(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("UInt64 array should downcast to UInt64Array")
                .value(row_index),
        )),
        DataType::Boolean => Ok(KeyValue::Boolean(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("Boolean array should downcast to BooleanArray")
                .value(row_index),
        )),
        DataType::Date32 => Ok(KeyValue::Date32(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("Date32 array should downcast to Date32Array")
                .value(row_index),
        )),
        DataType::Timestamp(TimeUnit::Second, _) => Ok(KeyValue::TimestampMicros(
            array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .expect("TimestampSecond array should downcast to TimestampSecondArray")
                .value(row_index)
                * 1_000_000,
        )),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Ok(KeyValue::TimestampMicros(
            array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("TimestampMillisecond array should downcast to TimestampMillisecondArray")
                .value(row_index)
                * 1_000,
        )),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(KeyValue::TimestampMicros(
            array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("TimestampMicrosecond array should downcast to TimestampMicrosecondArray")
                .value(row_index),
        )),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(KeyValue::TimestampMicros(
            array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("TimestampNanosecond array should downcast to TimestampNanosecondArray")
                .value(row_index)
                / 1_000,
        )),
        DataType::Decimal128(precision, scale) => Ok(KeyValue::Decimal(format!(
            "decimal128({precision},{scale}):{}",
            array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("Decimal128 array should downcast to Decimal128Array")
                .value(row_index)
        ))),
        DataType::Decimal256(precision, scale) => Ok(KeyValue::Decimal(format!(
            "decimal256({precision},{scale}):{}",
            array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .expect("Decimal256 array should downcast to Decimal256Array")
                .value(row_index)
        ))),
        other => Err(Error::UnsupportedColumnType {
            column: column.to_string(),
            data_type: other.to_string(),
        }),
    }
}

fn write_parquet_output(
    output_dir: &Path,
    plan: &PartitionPlan,
    stats: &StatsMetadata,
    assignments: &PartitionAssignmentSummary,
    dataset: &InputDataset,
) -> Result<WriteSummary> {
    fs::create_dir_all(output_dir).map_err(|source| Error::WriteFile {
        path: output_dir.to_path_buf(),
        source,
    })?;

    let mut assignments_by_partition = vec![Vec::new(); plan.output_partitions];
    for record in &assignments.records {
        if record.partition_id < assignments_by_partition.len() {
            assignments_by_partition[record.partition_id].push(record.clone());
        }
    }

    let mut output_files = Vec::new();
    for (partition_id, partition_assignments) in assignments_by_partition.iter().enumerate() {
        if partition_assignments.is_empty() {
            continue;
        }

        let relative_path = format!("rp_partition={partition_id}/part-{partition_id:05}.parquet");
        let file_path = output_dir.join(&relative_path);
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).map_err(|source| Error::WriteFile {
                path: parent.to_path_buf(),
                source,
            })?;
        }

        write_partition_parquet(&file_path, plan, dataset, partition_assignments)?;
        let size_bytes = fs::metadata(&file_path).ok().map(|metadata| metadata.len());
        output_files.push(OutputFile {
            path: relative_path,
            partition_id,
            row_count: partition_assignments.len() as u64,
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

fn write_partition_parquet(
    file_path: &Path,
    plan: &PartitionPlan,
    dataset: &InputDataset,
    assignments: &[RecordPartitionAssignment],
) -> Result<()> {
    if !dataset.batches.is_empty() {
        return write_retained_rows_parquet(
            file_path,
            dataset,
            assignments,
            &plan.technical_columns,
        );
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
            let values = assignments
                .iter()
                .map(|assignment| {
                    dataset
                        .rows
                        .rows
                        .get(assignment.row_index)
                        .and_then(|row| row.key_values().get(column))
                        .and_then(key_value_to_string)
                })
                .collect::<Vec<_>>();
            Arc::new(StringArray::from(values)) as ArrayRef
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(schema, columns)?;
    let batch = append_technical_columns(batch, assignments, &plan.technical_columns)?;
    let file = File::create(file_path).map_err(|source| Error::WriteFile {
        path: file_path.to_path_buf(),
        source,
    })?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

fn write_retained_rows_parquet(
    file_path: &Path,
    dataset: &InputDataset,
    assignments: &[RecordPartitionAssignment],
    technical_columns: &TechnicalColumns,
) -> Result<()> {
    let batches = retained_partition_batches(dataset, assignments, technical_columns)?;
    let schema = batches
        .first()
        .map(|batch| batch.schema())
        .unwrap_or_else(|| dataset.batches[0].schema());
    let file = File::create(file_path).map_err(|source| Error::WriteFile {
        path: file_path.to_path_buf(),
        source,
    })?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;

    for batch in batches {
        writer.write(&batch)?;
    }
    writer.close()?;

    Ok(())
}

fn retained_partition_batches(
    dataset: &InputDataset,
    assignments: &[RecordPartitionAssignment],
    technical_columns: &TechnicalColumns,
) -> Result<Vec<RecordBatch>> {
    let mut result = Vec::new();
    let mut batch_start = 0_usize;
    let mut row_cursor = 0_usize;

    for batch in &dataset.batches {
        let batch_end = batch_start + batch.num_rows();
        if row_cursor < assignments.len() && assignments[row_cursor].row_index < batch_start {
            return Err(Error::MissingRetainedRow {
                row_index: assignments[row_cursor].row_index,
            });
        }

        let mut local_indexes = Vec::new();
        let mut local_assignments = Vec::new();
        while row_cursor < assignments.len() && assignments[row_cursor].row_index < batch_end {
            local_indexes.push((assignments[row_cursor].row_index - batch_start) as u32);
            local_assignments.push(assignments[row_cursor].clone());
            row_cursor += 1;
        }

        if !local_indexes.is_empty() {
            let batch = take_batch(batch, &local_indexes)?;
            result.push(append_technical_columns(
                batch,
                &local_assignments,
                technical_columns,
            )?);
        }

        batch_start = batch_end;
    }

    if let Some(assignment) = assignments.get(row_cursor) {
        return Err(Error::MissingRetainedRow {
            row_index: assignment.row_index,
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

fn append_technical_columns(
    batch: RecordBatch,
    local_assignments: &[RecordPartitionAssignment],
    technical_columns: &TechnicalColumns,
) -> Result<RecordBatch> {
    if !technical_columns.included {
        return Ok(batch);
    }

    let partition_ids = local_assignments
        .iter()
        .map(|assignment| assignment.partition_id as u32)
        .collect::<Vec<_>>();
    let salts = local_assignments
        .iter()
        .map(|assignment| assignment.salt_index.map(|salt_index| salt_index as u32))
        .collect::<Vec<_>>();
    let is_heavy_key = local_assignments
        .iter()
        .map(|assignment| assignment.salt_index.is_some())
        .collect::<Vec<_>>();

    let mut fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new(
        &technical_columns.partition_column,
        DataType::UInt32,
        false,
    ));
    fields.push(Field::new(
        &technical_columns.salt_column,
        DataType::UInt32,
        true,
    ));
    fields.push(Field::new(
        &technical_columns.heavy_key_column,
        DataType::Boolean,
        false,
    ));

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(UInt32Array::from(partition_ids)) as ArrayRef);
    columns.push(Arc::new(UInt32Array::from(salts)) as ArrayRef);
    columns.push(Arc::new(BooleanArray::from(is_heavy_key)) as ArrayRef);

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}
