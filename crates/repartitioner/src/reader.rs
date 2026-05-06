use std::{collections::BTreeMap, fs, fs::File, path::Path};

use arrow_array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Decimal256Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeStringArray, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::{
    config::DatasetFormat,
    dataset::{Dataset, Row},
    key_encoding::KeyValue,
    Config, Error, Result,
};

#[derive(Debug, Clone, PartialEq)]
pub struct InputDataset {
    pub path: String,
    pub format: DatasetFormat,
    pub files: Vec<InputFile>,
    pub rows: Dataset,
    pub batches: Vec<RecordBatch>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputFile {
    pub path: String,
    pub size_bytes: u64,
}

impl InputDataset {
    pub fn from_rows(rows: Dataset) -> Self {
        Self {
            path: "<memory>".to_string(),
            format: DatasetFormat::Parquet,
            files: Vec::new(),
            rows,
            batches: Vec::new(),
        }
    }
}

pub fn read_dataset(config: &Config) -> Result<InputDataset> {
    match &config.dataset.format {
        DatasetFormat::Parquet => inspect_local_input(
            &config.dataset.input,
            DatasetFormat::Parquet,
            &config.partitioning.key_columns,
        ),
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
