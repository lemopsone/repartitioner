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

adaptive-partitioner \
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

## Integration

Spark should be used only to compare:

- baseline: Spark reads original dataset;
- preprocessed: Spark reads output dataset from the Rust tool.

The evaluation must report both:

- downstream Spark time only;
- total time = preprocessing time + downstream Spark time.