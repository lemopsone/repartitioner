use std::{collections::BTreeMap, fs, path::Path};

use crate::{
    config::DatasetFormat,
    dataset::{Dataset, Row},
    io::DatasetReader,
    key_encoding::KeyValue,
    reader::{InputDataset, InputFile},
    Config, Error, Result,
};

#[derive(Debug, Clone, Copy, Default)]
pub struct CsvDatasetReader;

impl DatasetReader for CsvDatasetReader {
    fn read_dataset(&self, config: &Config) -> Result<InputDataset> {
        read_csv_dataset(config)
    }
}

fn read_csv_dataset(config: &Config) -> Result<InputDataset> {
    let input = &config.dataset.input;
    if !input.is_file() {
        return Err(Error::InputPathNotFound {
            path: input.to_path_buf(),
        });
    }

    let input_file = inspect_file(input)?;
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(input)?;
    let headers = reader.headers()?.clone();
    let key_indexes = config
        .partitioning
        .key_columns
        .iter()
        .map(|column| {
            headers
                .iter()
                .position(|header| header == column)
                .ok_or_else(|| {
                    Error::UnsupportedFormat(format!("CSV input is missing key column: {column}"))
                })
        })
        .collect::<Result<Vec<_>>>()?;

    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record?;
        let mut key_values = BTreeMap::new();
        for (column, column_index) in config
            .partitioning
            .key_columns
            .iter()
            .zip(key_indexes.iter())
        {
            let value = record
                .get(*column_index)
                .map(|value| KeyValue::Utf8(value.to_string()))
                .unwrap_or(KeyValue::Null);
            key_values.insert(column.clone(), value);
        }
        rows.push(Row::new(key_values));
    }

    Ok(InputDataset {
        path: input.display().to_string(),
        format: DatasetFormat::Csv,
        files: vec![input_file],
        rows: Dataset::new(rows),
        batches: Vec::new(),
    })
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
