use std::{fs::File, path::Path, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use repartitioner::{
    config::Config, partitioner, planner, reader, statistics, writer, Error, Result,
};

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
fn rejects_missing_input_path() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let missing = tempdir.path().join("missing.parquet");
    let config = config_for(&missing, &tempdir.path().join("out"));

    let error = reader::read_dataset(&config).expect_err("missing input should fail");

    assert!(matches!(error, Error::InputPathNotFound { .. }));
}

#[test]
fn writes_partitioned_parquet_dataset_and_reads_it_back() -> Result<()> {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let input = tempdir.path().join("input.parquet");
    let output = tempdir.path().join("partitioned");
    let values = std::iter::repeat_n("heavy", 24)
        .chain(["a", "b", "c", "d", "e", "f", "g", "h"])
        .collect::<Vec<_>>();
    write_user_id_with_payload_parquet(&input, &values)?;

    let config = config_for(&input, &output);
    let dataset = reader::read_dataset(&config)?;
    let mut stats = statistics::compute_statistics(&config, &dataset)?;
    let plan = planner::build_plan(&config, &stats)?;
    let assignments = partitioner::assign_partitions(&plan, &dataset)?;
    stats.set_after_partition_sizes(assignments.partition_row_counts.clone());
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
    assert_output_schema_contains_payload_columns(&output, &write_summary.manifest.output_files);
    assert_plan_metadata_contains_adaptive_partitioning(&output);
    assert_stats_metadata_contains_after_partition_sizes(&output);

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

fn write_user_id_with_payload_parquet(path: &Path, values: &[&str]) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("row_id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
    ]));
    let user_ids = values.iter().map(|value| Some(*value)).collect::<Vec<_>>();
    let row_ids = (0..values.len() as i64).collect::<Vec<_>>();
    let regions = values
        .iter()
        .enumerate()
        .map(|(index, _)| Some(if index % 2 == 0 { "north" } else { "south" }))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(user_ids)) as ArrayRef,
            Arc::new(Int64Array::from(row_ids)) as ArrayRef,
            Arc::new(StringArray::from(regions)) as ArrayRef,
        ],
    )?;
    let file = File::create(path).expect("input parquet should be created");
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

fn assert_output_schema_contains_payload_columns(
    output: &Path,
    output_files: &[repartitioner::manifest::OutputFile],
) {
    let first_file = output.join(&output_files.first().expect("at least one output file").path);
    let file = File::open(first_file).expect("output parquet should open");
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("output parquet reader should build");
    let mut reader = builder.build().expect("output parquet reader should build");
    let batch = reader
        .next()
        .expect("output parquet should contain a batch")
        .expect("output parquet batch should read");

    assert_eq!(batch.schema().field(0).name(), "user_id");
    assert!(batch.schema().field_with_name("row_id").is_ok());
    assert!(batch.schema().field_with_name("region").is_ok());
}

fn assert_stats_metadata_contains_after_partition_sizes(output: &Path) {
    let payload = std::fs::read_to_string(output.join("_stats.json"))
        .expect("stats metadata should be readable");
    let metadata: serde_json::Value =
        serde_json::from_str(&payload).expect("stats metadata should parse");
    let after_sizes = metadata["estimates"]["after_partition_sizes"]
        .as_array()
        .expect("after partition sizes should be an array");

    let plan_payload = std::fs::read_to_string(output.join("_partition_plan.json"))
        .expect("plan metadata should be readable");
    let plan_metadata: serde_json::Value =
        serde_json::from_str(&plan_payload).expect("plan metadata should parse");
    let output_partitions = plan_metadata["output_partitions"]
        .as_u64()
        .expect("output partitions should be numeric") as usize;

    assert_eq!(after_sizes.len(), output_partitions);
    assert!(
        after_sizes
            .iter()
            .map(|value| value.as_u64().unwrap_or_default())
            .sum::<u64>()
            > 0
    );
}

fn assert_plan_metadata_contains_adaptive_partitioning(output: &Path) {
    let payload = std::fs::read_to_string(output.join("_partition_plan.json"))
        .expect("plan metadata should be readable");
    let metadata: serde_json::Value =
        serde_json::from_str(&payload).expect("plan metadata should parse");

    assert_eq!(metadata["min_partitions"].as_u64(), Some(1));
    assert_eq!(metadata["max_partitions"].as_u64(), Some(4));
    assert!(metadata["required_partitions_by_size"].as_u64().is_some());
    assert!(metadata["output_partitions"].as_u64().unwrap_or_default() <= 4);
    assert!(metadata["feasibility"].is_object());
}
