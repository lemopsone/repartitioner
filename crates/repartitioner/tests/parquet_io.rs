use std::{fs::File, path::Path, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray, UInt32Array,
};
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
        stats
            .metadata
            .input
            .key_frequencies
            .get("7:user_id#utf8:5:heavy"),
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
fn reads_int64_key_rows_and_partitions_them() -> Result<()> {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let input = tempdir.path().join("input_int64.parquet");
    write_int64_user_id_parquet(&input, &[42, 42, 7, 9])?;

    let config = config_for(&input, &tempdir.path().join("out"));
    let dataset = reader::read_dataset(&config)?;
    let stats = statistics::compute_statistics(&config, &dataset)?;
    let plan = planner::build_plan(&config, &stats)?;
    let assignments = partitioner::assign_partitions(&plan, &dataset)?;

    assert_eq!(dataset.rows.row_count(), 4);
    assert_eq!(
        stats
            .metadata
            .input
            .key_frequencies
            .get("7:user_id#int64:42"),
        Some(&2)
    );
    assert_eq!(assignments.partition_row_counts.iter().sum::<u64>(), 4);

    Ok(())
}

#[test]
fn keeps_null_and_empty_string_as_distinct_keys() -> Result<()> {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let input = tempdir.path().join("nullable_strings.parquet");
    write_nullable_user_id_parquet(&input, &[None, Some(""), Some("heavy")])?;

    let config = config_for(&input, &tempdir.path().join("out"));
    let dataset = reader::read_dataset(&config)?;
    let stats = statistics::compute_statistics(&config, &dataset)?;

    assert_eq!(
        stats.metadata.input.key_frequencies.get("7:user_id#null"),
        Some(&1)
    );
    assert_eq!(
        stats
            .metadata
            .input
            .key_frequencies
            .get("7:user_id#utf8:0:"),
        Some(&1)
    );

    Ok(())
}

#[test]
fn encodes_composite_keys_with_delimiters_deterministically() -> Result<()> {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let input = tempdir.path().join("composite.parquet");
    write_composite_string_parquet(&input, &[("a=b|c", "eu"), ("a=b|c", "eu"), ("x", "us")])?;

    let config =
        config_for_key_columns(&input, &tempdir.path().join("out"), &["user_id", "region"]);
    let dataset = reader::read_dataset(&config)?;
    let first = statistics::compute_statistics(&config, &dataset)?;
    let second = statistics::compute_statistics(&config, &dataset)?;

    let encoded = "7:user_id#utf8:5:a=b|c|6:region#utf8:2:eu";
    assert_eq!(first.metadata.input.key_frequencies.get(encoded), Some(&2));
    assert_eq!(
        first.metadata.input.key_frequencies,
        second.metadata.input.key_frequencies
    );

    Ok(())
}

#[test]
fn detects_heavy_hitter_for_numeric_key() -> Result<()> {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let input = tempdir.path().join("numeric_heavy.parquet");
    write_int64_user_id_parquet(&input, &[42, 42, 42, 42, 42, 42, 1, 2])?;

    let config = config_for(&input, &tempdir.path().join("out"));
    let dataset = reader::read_dataset(&config)?;
    let stats = statistics::compute_statistics(&config, &dataset)?;
    let plan = planner::build_plan(&config, &stats)?;

    assert!(plan
        .metadata
        .heavy_keys
        .iter()
        .any(|key| key.key == "7:user_id#int64:42"));

    Ok(())
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
    assert_output_contains_technical_columns(&output, &write_summary.manifest.output_files);
    assert_plan_metadata_contains_adaptive_partitioning(&output);
    assert_stats_metadata_contains_after_partition_sizes(&output);

    Ok(())
}

#[test]
fn omits_technical_columns_when_disabled() -> Result<()> {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let input = tempdir.path().join("input.parquet");
    let output = tempdir.path().join("partitioned_without_technical_columns");
    let values = std::iter::repeat_n("heavy", 24)
        .chain(["a", "b", "c", "d", "e", "f", "g", "h"])
        .collect::<Vec<_>>();
    write_user_id_with_payload_parquet(&input, &values)?;

    let config = config_for_without_technical_columns(&input, &output);
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

    let first_batch = read_first_output_batch(&output, &write_summary.manifest.output_files);

    assert!(first_batch.schema().field_with_name("user_id").is_ok());
    assert!(first_batch.schema().field_with_name("row_id").is_ok());
    assert!(first_batch.schema().field_with_name("region").is_ok());
    assert!(first_batch
        .schema()
        .field_with_name("_rp_partition_id")
        .is_err());
    assert!(first_batch.schema().field_with_name("_rp_salt").is_err());
    assert!(first_batch
        .schema()
        .field_with_name("_rp_is_heavy_key")
        .is_err());

    Ok(())
}

#[test]
fn writes_no_op_metadata_without_output_parquet_files() -> Result<()> {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let input = tempdir.path().join("input.parquet");
    let output = tempdir.path().join("no_op");
    write_user_id_parquet(&input, &["a", "a", "b", "b"])?;

    let config = config_for(&input, &output);
    let dataset = reader::read_dataset(&config)?;
    let mut stats = statistics::compute_statistics(&config, &dataset)?;
    let plan = planner::build_plan(&config, &stats)?;
    stats.set_after_partition_sizes(stats.metadata.estimates.before_partition_sizes.clone());

    assert!(!plan.metadata.rewrite_required);
    assert_eq!(plan.metadata.cost_estimate.estimated_rows_written, 0);

    let write_summary =
        writer::write_no_op_output(&output, &plan.metadata, &stats.metadata, &dataset)?;

    assert!(output.join("_partition_plan.json").is_file());
    assert!(output.join("_stats.json").is_file());
    assert!(output.join("_manifest.json").is_file());
    assert!(write_summary.manifest.input_reused);
    assert_eq!(
        write_summary.manifest.dataset_location.as_deref(),
        Some(input.to_string_lossy().as_ref())
    );
    assert!(write_summary.manifest.output_files.is_empty());
    assert!(!contains_parquet_file(&output));

    Ok(())
}

fn config_for(input: &Path, output: &Path) -> Config {
    config_for_key_columns(input, output, &["user_id"])
}

fn config_for_key_columns(input: &Path, output: &Path, key_columns: &[&str]) -> Config {
    let key_columns = key_columns
        .iter()
        .map(|column| format!("\"{column}\""))
        .collect::<Vec<_>>()
        .join(", ");

    Config::from_yaml_str(&format!(
        r#"
dataset:
  input: "{}"
  output: "{}"
  format: "parquet"

partitioning:
  key_columns: [{key_columns}]
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

fn config_for_without_technical_columns(input: &Path, output: &Path) -> Config {
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

output:
  include_technical_columns: false

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

fn write_nullable_user_id_parquet(path: &Path, values: &[Option<&str>]) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "user_id",
        DataType::Utf8,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(values.to_vec())) as ArrayRef],
    )?;
    let file = File::create(path).expect("input parquet should be created");
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

fn write_int64_user_id_parquet(path: &Path, values: &[i64]) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "user_id",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(values.to_vec())) as ArrayRef],
    )?;
    let file = File::create(path).expect("input parquet should be created");
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

fn write_composite_string_parquet(path: &Path, values: &[(&str, &str)]) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
    ]));
    let user_ids = values
        .iter()
        .map(|(user_id, _)| Some(*user_id))
        .collect::<Vec<_>>();
    let regions = values
        .iter()
        .map(|(_, region)| Some(*region))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(user_ids)) as ArrayRef,
            Arc::new(StringArray::from(regions)) as ArrayRef,
        ],
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
    let batch = read_first_output_batch(output, output_files);

    assert_eq!(batch.schema().field(0).name(), "user_id");
    assert!(batch.schema().field_with_name("row_id").is_ok());
    assert!(batch.schema().field_with_name("region").is_ok());
}

fn assert_output_contains_technical_columns(
    output: &Path,
    output_files: &[repartitioner::manifest::OutputFile],
) {
    let mut saw_heavy = false;
    let mut saw_normal = false;

    for output_file in output_files {
        for batch in read_output_batches(output, output_file) {
            assert!(batch.schema().field_with_name("_rp_partition_id").is_ok());
            assert!(batch.schema().field_with_name("_rp_salt").is_ok());
            assert!(batch.schema().field_with_name("_rp_is_heavy_key").is_ok());

            let user_ids = batch
                .column_by_name("user_id")
                .expect("user_id column should exist")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("user_id should be utf8");
            let partition_ids = batch
                .column_by_name("_rp_partition_id")
                .expect("partition id column should exist")
                .as_any()
                .downcast_ref::<UInt32Array>()
                .expect("partition id should be UInt32");
            let salts = batch
                .column_by_name("_rp_salt")
                .expect("salt column should exist")
                .as_any()
                .downcast_ref::<UInt32Array>()
                .expect("salt should be UInt32");
            let is_heavy_key = batch
                .column_by_name("_rp_is_heavy_key")
                .expect("heavy key column should exist")
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("heavy key flag should be boolean");

            for row_index in 0..batch.num_rows() {
                assert_eq!(
                    partition_ids.value(row_index) as usize,
                    output_file.partition_id
                );

                if user_ids.value(row_index) == "heavy" {
                    saw_heavy = true;
                    assert!(!salts.is_null(row_index));
                    assert!(is_heavy_key.value(row_index));
                } else {
                    saw_normal = true;
                    assert!(salts.is_null(row_index));
                    assert!(!is_heavy_key.value(row_index));
                }
            }
        }
    }

    assert!(saw_heavy);
    assert!(saw_normal);
}

fn read_first_output_batch(
    output: &Path,
    output_files: &[repartitioner::manifest::OutputFile],
) -> RecordBatch {
    let first_file = output_files.first().expect("at least one output file");
    read_output_batches(output, first_file)
        .into_iter()
        .next()
        .expect("output parquet should contain a batch")
}

fn read_output_batches(
    output: &Path,
    output_file: &repartitioner::manifest::OutputFile,
) -> Vec<RecordBatch> {
    let file = File::open(output.join(&output_file.path)).expect("output parquet should open");
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("output parquet reader should build");
    let reader = builder.build().expect("output parquet reader should build");

    reader
        .map(|batch| batch.expect("output parquet batch should read"))
        .collect()
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
    assert_eq!(
        metadata["technical_columns"]["included"].as_bool(),
        Some(true)
    );
    assert_eq!(
        metadata["technical_columns"]["partition_column"].as_str(),
        Some("_rp_partition_id")
    );
    assert_eq!(
        metadata["technical_columns"]["salt_column"].as_str(),
        Some("_rp_salt")
    );
    assert_eq!(
        metadata["technical_columns"]["heavy_key_column"].as_str(),
        Some("_rp_is_heavy_key")
    );
}

fn contains_parquet_file(dir: &Path) -> bool {
    std::fs::read_dir(dir)
        .expect("output directory should be readable")
        .filter_map(|entry| entry.ok())
        .any(|entry| {
            let path = entry.path();
            if path.is_dir() {
                contains_parquet_file(&path)
            } else {
                path.extension()
                    .is_some_and(|extension| extension == std::ffi::OsStr::new("parquet"))
            }
        })
}
