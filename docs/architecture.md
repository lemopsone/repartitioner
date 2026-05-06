# Architecture

## Overview

The prototype consists of two parts:

1. Rust preprocessing core.
2. Python/Spark experimental harness.

The Rust core is the developed software product. Spark is not part of the core method.

## Applicability

The prototype supports the following scope:

| Area | Status |
| --- | --- |
| Adaptive hash/salt repartitioning | Fully implemented in Rust core |
| No-op decision | Implemented; metadata is written without Parquet rewrite |
| Parquet input/output | Full prototype support |
| CSV input | Supported for statistics/planning |
| CSV output | Not implemented |
| Spark groupBy benchmark | Baseline, physical-only, method-aware |
| Spark join benchmark | Baseline, physical-only, experimental method-aware |
| Standalone file-size balancing strategy | Not implemented; writer file rolling only |

The implemented Rust method should therefore be described as adaptive key
placement with selective salting and optional no-op, not as a general-purpose
file compaction engine or a Spark extension.

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
`io/mod.rs`. `reader.rs` and `writer.rs` are facade modules: `reader.rs`
selects the adapter from `dataset.input_format`, and `writer.rs` selects the
adapter from `output.format`.

The current adapter is `io/parquet.rs`. It owns all Arrow/Parquet-specific
logic: file discovery, Parquet batch reading, Arrow key extraction, Hive-style
`rp_partition=<id>` output directories, and Parquet file writing.

Parquet is the only full read/write adapter in the current prototype. CSV is an
input adapter for statistics and planning; CSV output is not implemented. A
supported mixed-format scenario is CSV input with Parquet output.

Output file sizing is implemented in the Parquet writer: large logical
partitions can be written as multiple `part-xxxxx.parquet` files according to
`storage.target_file_size_mb`. This is not a standalone file-size balancing
planner strategy. `partitioning.strategy: "file_size_balancing"` is currently
reserved for future split/coalesce planning and is rejected with a clear error.

Parquet processing is two-pass:

- pass 1 scans only configured key columns using Parquet projection and retains
  encoded key rows for statistics and planning;
- pass 2 scans full record batches again and writes output batches directly to
  lazily opened partition writers.

This means full Arrow `RecordBatch` values are no longer retained in
`InputDataset` for normal Parquet input. The current prototype still keeps
encoded keys and row assignments in memory; replacing those structures with a
fully streaming/state-only planner is a separate optimization.

The method core remains outside the adapter layer:

- `statistics.rs`: computes statistics over `InputDataset`.
- `planner.rs`: builds the partitioning plan.
- `partitioner.rs`: assigns rows to planned partitions.

Additional formats such as CSV, ORC, or Avro should be added as new adapters
implementing the same traits, without changing the statistics/planner/
partitioner logic.

## Approximate heavy hitter mode

`statistics.heavy_hitter_mode: "exact"` is the default and materializes the full
key frequency map. In this mode `_stats.json` reports exact `distinct_keys`,
exact `key_frequencies`, and allows the planner to build a complete
load-aware normal-key assignment.

`statistics.heavy_hitter_mode: "approximate"` uses a bounded Space-Saving
summary for heavy-key detection. The summary is not a complete key
distribution, so metadata marks it explicitly:

- `heavy_hitter_detection.exact = false`;
- `heavy_hitter_detection.frequencies_truncated = true`;
- `input.key_frequencies_exact = false`;
- `input.key_frequencies_truncated = true`;
- `input.normal_keys_materialized = false`;
- `input.distinct_keys = null`.

In approximate mode the planner supports heavy-key salting, but it does not
build a full `normal_keys` plan from the top-K summary. If
`normal_key_assignment: "load_aware"` is configured, the effective normal-key
assignment falls back to hash placement and the plan records the note
`load_aware_normal_assignment_disabled_in_approximate_mode`.

## CLI

Example command:

repartitioner \
  --input ./data/input.parquet \
  --output ./data/output_partitioned \
  --config ./configs/heavy-key.yaml

## Config example

```yaml
dataset:
  input: "./data/input.csv"
  input_format: "csv"

output:
  path: "./data/output_partitioned"
  format: "parquet"
  include_technical_columns: true
  partition_column: "_rp_partition_id"
  salt_column: "_rp_salt"
  heavy_key_column: "_rp_is_heavy_key"

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
```

Legacy configs remain valid:

```yaml
dataset:
  input: "./data/input.parquet"
  output: "./data/output_partitioned"
  format: "parquet"
```

In the legacy form, `dataset.format` is interpreted as `dataset.input_format`,
`dataset.output` is interpreted as `output.path`, and `output.format` defaults
to `dataset.format`.

## Resource Guard

The current prototype uses in-memory processing for encoded keys and row
assignment. Full Parquet payload batches are streamed during writing, but
`resources.memory_limit_mb` is still used as an explicit guard against
unexpectedly large inputs.

During statistics computation the tool estimates dataset size from input file
sizes and writes a `resources` section to `_stats.json`:

- configured local thread count;
- local thread count used by the current execution model;
- whether parallel execution is enabled;
- configured memory limit;
- estimated dataset size in MB;
- whether in-memory processing is used;
- whether the configured limit is exceeded;
- resource warnings.

The prototype currently does not use Rayon or a CPU thread pool, so
`parallel_execution_enabled` is `false`. `resources.local_threads` is still
applied as a deterministic cap for the number of simultaneously open Parquet
partition writers in the streaming output path. Metadata records the configured
value and adds a warning when the value is not used for true parallel
execution.

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
partial aggregation by `["_rp_partition_id", "_rp_salt", ...key_columns]`,
followed by final aggregation by the original key columns.

For `join`, the plan recommends method-aware salted heavy-key join logic using
the materialized technical columns `_rp_salt` and `_rp_is_heavy_key`. Join
metadata can recommend `broadcast_join`, `salted_heavy_key_join`,
`heavy_key_isolation_join`, or `physical_repartitioning`. The Spark benchmark
implements these as an experimental validation path for single-column join
keys; unsupported composite cases are skipped with explicit reasons.

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
