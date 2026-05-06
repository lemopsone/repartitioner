use arrow_array::RecordBatch;

use crate::{
    config::DatasetFormat,
    dataset::Dataset,
    io::{csv::CsvDatasetReader, parquet::ParquetDatasetReader, DatasetReader},
    Config, Result,
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
        DatasetFormat::Parquet => ParquetDatasetReader.read_dataset(config),
        DatasetFormat::Csv => CsvDatasetReader.read_dataset(config),
    }
}
