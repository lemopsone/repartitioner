# Architecture

## Overview

The prototype consists of two parts:

1. Rust preprocessing core.
2. Python/Spark experimental harness.

The Rust core is the developed software product. Spark is not part of the core method.

## Rust core

Expected crate layout:

src/
- main.rs
- cli.rs
- config.rs
- io/mod.rs
- io/parquet.rs
- reader.rs
- statistics.rs
- heavy_hitters.rs
- planner.rs
- partitioner.rs
- writer.rs
- manifest.rs
- error.rs

## I/O adapter layer

The adaptive partitioning method is format-independent. The Rust prototype uses
Parquet as its first concrete dataset format, but Parquet is an implementation
adapter rather than a constraint of the method.

The crate exposes format-agnostic `DatasetReader` and `DatasetWriter` traits in
`io/mod.rs`. `reader.rs` and `writer.rs` are facade modules: they select the
adapter from `dataset.format` and keep the CLI behavior stable.

The current adapter is `io/parquet.rs`. It owns all Arrow/Parquet-specific
logic: file discovery, Parquet batch reading, Arrow key extraction, retained
batch rewriting, Hive-style `rp_partition=<id>` output directories, and Parquet
file writing.

The method core remains outside the adapter layer:

- `statistics.rs`: computes statistics over `InputDataset`.
- `planner.rs`: builds the partitioning plan.
- `partitioner.rs`: assigns rows to planned partitions.

Additional formats such as CSV, ORC, or Avro should be added as new adapters
implementing the same traits, without changing the statistics/planner/
partitioner logic.

## CLI

Example command:

repartitioner \
  --input ./data/input.parquet \
  --output ./data/output_partitioned \
  --config ./configs/heavy-key.yaml

## Config example

dataset:
  input: "./data/input.parquet"
  output: "./data/output_partitioned"
  format: "parquet"

partitioning:
  key_columns: ["user_id"]
  target_partition_size_mb: 128
  max_partitions: 128
  strategy: "adaptive_hash_salt"
  heavy_key_alpha: 2.0
  seed: 42

job:
  type: "group_by"
  downstream_engine: "spark"

resources:
  local_threads: 8
  memory_limit_mb: 4096
  fail_on_memory_limit: false

## Resource Guard

The current prototype uses in-memory processing for method statistics and row
assignment. `resources.memory_limit_mb` is therefore used as an explicit guard
against unexpectedly large inputs.

During statistics computation the tool estimates dataset size from input file
sizes and writes a `resources` section to `_stats.json`:

- configured memory limit;
- estimated dataset size in MB;
- whether in-memory processing is used;
- whether the configured limit is exceeded;
- resource warnings.

If `resources.fail_on_memory_limit` is `false`, the tool continues and records a
warning. If it is `true`, statistics computation fails with
`ResourceLimitExceeded` before planning and writing.

## Job-aware planning

`job.type` is part of the method input and is persisted in
`_partition_plan.json` together with `recommended_downstream_plan`.

For `scan`, `filter`, and `generic` jobs, the Rust tool recommends physical
repartitioning only. The downstream Spark operator does not need to be
rewritten, although filter selectivity is currently not estimated and is
reported as a plan note.

For `group_by`, the plan recommends method-aware two-stage aggregation:
partial aggregation by `["_rp_partition_id", ...key_columns]`, followed by
final aggregation by the original key columns.

For `join`, the plan recommends method-aware salted heavy-key join logic using
the materialized technical columns `_rp_salt` and `_rp_is_heavy_key`.

Physical rewriting alone can improve scan/filter locality and file layout, but
group_by/join skew mitigation requires downstream logic that consumes the
technical columns from the repartitioned dataset.

## Integration

Spark should be used only to compare:

- baseline: Spark reads original dataset;
- preprocessed: Spark reads output dataset from the Rust tool.

The evaluation must report both:

- downstream Spark time only;
- total time = preprocessing time + downstream Spark time.
