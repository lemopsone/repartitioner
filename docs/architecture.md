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
- reader.rs
- statistics.rs
- heavy_hitters.rs
- planner.rs
- partitioner.rs
- writer.rs
- manifest.rs
- error.rs

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
