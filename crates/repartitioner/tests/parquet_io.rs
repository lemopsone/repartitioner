use std::{fs::File, path::Path, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use repartitioner::{config::Config, partitioner, planner, reader, statistics, writer, Result};

#[test]
fn reads_key_rows_from_temporary_parquet_file() -> Result<()> {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let input = tempdir.path().join("input.parquet");
    write_user_id_parquet(&input, &["a", "a", "b", "heavy", "heavy", "heavy"])?;

    let config = config_for(&input, &tempdir.path().join("out"));
    let dataset = reader::read_dataset(&config)?;
    let stats = statistics::compute_statistics(&config, &dataset)?;

    assert_eq!(dataset.rows.row_count(), 6);
    assert_eq!(stats.metadata.input.distinct_keys, Some(3));
    assert_eq!(
        stats.metadata.input.key_frequencies.get("user_id=heavy"),
        Some(&3)
    );

    Ok(())
}

#[test]
fn writes_partitioned_parquet_dataset_and_reads_it_back() -> Result<()> {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let input = tempdir.path().join("input.parquet");
    let output = tempdir.path().join("partitioned");
    let values = std::iter::repeat("heavy")
        .take(24)
        .chain(["a", "b", "c", "d", "e", "f", "g", "h"])
        .collect::<Vec<_>>();
    write_user_id_parquet(&input, &values)?;

    let config = config_for(&input, &output);
    let dataset = reader::read_dataset(&config)?;
    let stats = statistics::compute_statistics(&config, &dataset)?;
    let plan = planner::build_plan(&config, &stats)?;
    let assignments = partitioner::assign_partitions(&plan, &dataset)?;
    let write_summary = writer::write_output(
        &output,
        &plan.metadata,
        &stats.metadata,
        &assignments,
        &dataset,
    )?;

    assert!(output.join("_partition_plan.json").is_file());
    assert!(output.join("_stats.json").is_file());
    assert!(output.join("_manifest.json").is_file());
    assert!(!write_summary.manifest.output_files.is_empty());
    assert!(write_summary
        .manifest
        .output_files
        .iter()
        .all(|file| output.join(&file.path).is_file()));

    let read_back_config = config_for(&output, &tempdir.path().join("unused"));
    let read_back = reader::read_dataset(&read_back_config)?;

    assert_eq!(read_back.rows.row_count(), dataset.rows.row_count());

    Ok(())
}

fn config_for(input: &Path, output: &Path) -> Config {
    Config::from_yaml_str(&format!(
        r#"
dataset:
  input: "{}"
  output: "{}"
  format: "parquet"

partitioning:
  key_columns: ["user_id"]
  target_partition_size_mb: 128
  max_partitions: 4
  strategy: "adaptive_hash_salt"
  heavy_key_alpha: 2.0
  seed: 42

job:
  type: "group_by"
  downstream_engine: "spark"

resources:
  local_threads: 2
  memory_limit_mb: 1024
"#,
        input.display(),
        output.display()
    ))
    .expect("test config should be valid")
}

fn write_user_id_parquet(path: &Path, values: &[&str]) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "user_id",
        DataType::Utf8,
        false,
    )]));
    let user_ids = values.iter().map(|value| Some(*value)).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(user_ids)) as ArrayRef],
    )?;
    let file = File::create(path).expect("input parquet should be created");
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}
