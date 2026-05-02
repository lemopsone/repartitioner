use std::{collections::BTreeMap, fs, fs::File, path::Path};

use arrow_array::{Array, LargeStringArray, StringArray};
use arrow_schema::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::{
    config::DatasetFormat,
    dataset::{Dataset, Row},
    Config, Error, Result,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputDataset {
    pub path: String,
    pub format: DatasetFormat,
    pub files: Vec<InputFile>,
    pub rows: Dataset,
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
        Vec::new()
    };
    let rows = read_parquet_rows(&files, key_columns)?;

    Ok(InputDataset {
        path: input.display().to_string(),
        format,
        files,
        rows,
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
                .map_or(false, |ext| ext == std::ffi::OsStr::new("parquet"))
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

fn read_parquet_rows(files: &[InputFile], key_columns: &[String]) -> Result<Dataset> {
    let mut rows = Vec::new();

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
                    let value = string_value(column, array.as_ref(), row_index)?;
                    key_values.insert(column.clone(), value);
                }
                rows.push(Row::new(key_values));
            }
        }
    }

    Ok(Dataset::new(rows))
}

fn string_value(column: &str, array: &dyn Array, row_index: usize) -> Result<String> {
    if array.is_null(row_index) {
        return Ok(String::new());
    }

    match array.data_type() {
        DataType::Utf8 => Ok(array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 array should downcast to StringArray")
            .value(row_index)
            .to_string()),
        DataType::LargeUtf8 => Ok(array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .expect("LargeUtf8 array should downcast to LargeStringArray")
            .value(row_index)
            .to_string()),
        other => Err(Error::UnsupportedColumnType {
            column: column.to_string(),
            data_type: other.to_string(),
        }),
    }
}
