use std::{fs, path::Path};

use crate::{config::DatasetFormat, Config, Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputDataset {
    pub path: String,
    pub format: DatasetFormat,
    pub files: Vec<InputFile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputFile {
    pub path: String,
    pub size_bytes: u64,
}

pub fn read_dataset(config: &Config) -> Result<InputDataset> {
    match &config.dataset.format {
        DatasetFormat::Parquet => {
            inspect_local_input(&config.dataset.input, DatasetFormat::Parquet)
        }
    }
}

fn inspect_local_input(input: &str, format: DatasetFormat) -> Result<InputDataset> {
    let path = Path::new(input);

    let files = if path.is_file() {
        vec![inspect_file(path)?]
    } else if path.is_dir() {
        let mut files = Vec::new();
        for entry in fs::read_dir(path).map_err(|source| Error::ReadFile {
            path: path.to_path_buf(),
            source,
        })? {
            let entry = entry.map_err(|source| Error::ReadFile {
                path: path.to_path_buf(),
                source,
            })?;
            let entry_path = entry.path();
            if entry_path.is_file()
                && entry_path
                    .extension()
                    .map_or(false, |ext| ext == std::ffi::OsStr::new("parquet"))
            {
                files.push(inspect_file(&entry_path)?);
            }
        }
        files.sort_by(|left, right| left.path.cmp(&right.path));
        files
    } else {
        Vec::new()
    };

    Ok(InputDataset {
        path: input.to_string(),
        format,
        files,
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
