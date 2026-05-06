use std::path::Path;

use repartitioner::{
    config::Config, partitioner, planner, reader, statistics, writer, Error, Result,
};

#[test]
fn reads_csv_key_column_and_builds_plan() -> Result<()> {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let input = tempdir.path().join("input.csv");
    std::fs::write(
        &input,
        "user_id,region,payload\nheavy,eu,1\nheavy,us,2\na,eu,3\n",
    )
    .expect("csv input should be written");

    let config = csv_config_for(&input, &tempdir.path().join("out"));
    let dataset = reader::read_dataset(&config)?;
    let stats = statistics::compute_statistics(&config, &dataset)?;
    let plan = planner::build_plan(&config, &stats)?;

    assert_eq!(dataset.rows.row_count(), 3);
    assert_eq!(stats.metadata.input.distinct_keys, Some(2));
    assert_eq!(
        stats
            .metadata
            .input
            .key_frequencies
            .get("7:user_id#utf8:5:heavy"),
        Some(&2)
    );
    assert_eq!(plan.metadata.key_columns, vec!["user_id".to_string()]);

    Ok(())
}

#[test]
fn csv_output_reports_explicit_not_implemented_error() -> Result<()> {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let input = tempdir.path().join("input.csv");
    std::fs::write(&input, "user_id\nheavy\nheavy\na\n").expect("csv input should be written");

    let config = csv_config_for(&input, &tempdir.path().join("out"));
    let dataset = reader::read_dataset(&config)?;
    let mut stats = statistics::compute_statistics(&config, &dataset)?;
    let plan = planner::build_plan(&config, &stats)?;
    let assignments = partitioner::assign_partitions(&plan, &dataset)?;
    stats.set_after_partition_sizes(assignments.partition_row_counts.clone());

    let error = writer::write_output(
        &config.dataset.output,
        &plan.metadata,
        &stats.metadata,
        &assignments,
        &dataset,
    )
    .expect_err("csv output should not be implemented");

    assert!(matches!(
        error,
        Error::UnsupportedFormat(message)
            if message == "CSV output is not implemented; use parquet output"
    ));

    Ok(())
}

fn csv_config_for(input: &Path, output: &Path) -> Config {
    Config::from_yaml_str(&format!(
        r#"
dataset:
  input: "{}"
  output: "{}"
  format: "csv"

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
    .expect("csv test config should be valid")
}
