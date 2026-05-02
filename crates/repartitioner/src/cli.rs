use std::path::PathBuf;

use clap::Parser;

use crate::{config::Config, Result};

#[derive(Debug, Parser)]
#[command(
    name = "adaptive-partitioner",
    version,
    about = "External preprocessing tool for adaptive Parquet repartitioning"
)]
pub struct CliArgs {
    #[arg(long, value_name = "PATH")]
    pub input: Option<PathBuf>,

    #[arg(long, value_name = "DIR")]
    pub output: Option<PathBuf>,

    #[arg(long, value_name = "YAML")]
    pub config: PathBuf,
}

impl CliArgs {
    pub fn load_config(&self) -> Result<Config> {
        let mut config = Config::from_yaml_file(&self.config)?;

        if let Some(input) = &self.input {
            config.dataset.input = input.clone();
        }

        if let Some(output) = &self.output {
            config.dataset.output = output.clone();
        }

        config.validate()?;

        Ok(config)
    }

    pub fn parse_args() -> Self {
        Self::parse()
    }
}
