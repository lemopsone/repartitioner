# Method specification

## Goal

Build an adaptive partitioning plan from computed statistics and use it to
rewrite the input dataset into a more balanced physical layout when rewriting
is justified. If the input layout already satisfies the target bounds and skew
constraints, the method may choose `no_op` and write metadata only.

The implemented method is:

- adaptive key placement for normal keys;
- selective salting for heavy keys;
- optional no-op to avoid unnecessary rewrite cost;
- job-aware metadata for downstream scan/filter/group_by/join workloads.

The method is format-independent at the conceptual level. The prototype uses
concrete I/O adapters to demonstrate the method on local datasets.

## Input

- Dataset D.
- Key columns K.
- Target partition size L.
- Target or maximum number of partitions N.
- Resource constraints R.
- Downstream job type C:
  - scan;
  - group_by;
  - join;
  - filter;
  - generic.

## Output

- Repartitioned dataset D', or reused input dataset when action is `no_op`.
- Partitioning plan P.
- Statistics file S.
- Manifest M.

## Method And Prototype Scope

Method-level model:

- input dataset D;
- key columns K;
- downstream job parameters C;
- resource controls R;
- target partition bound L;
- maximum output partition count N;
- adaptive partitioning plan P.

Prototype support:

- Parquet input: full read support.
- Parquet output: full write support with Spark-compatible Hive-style
  `rp_partition=<id>` directories.
- CSV input: supported for statistics and planning.
- CSV output: not implemented; use `output.format: "parquet"`.
- Spark groupBy benchmark: baseline, physical-only, and method-aware modes.
- Spark join benchmark: baseline, physical-only, and experimental
  method-aware modes for safe single-key scenarios.
- Standalone `file_size_balancing` planner strategy: not implemented.

## Statistics

The tool should collect:

- total number of rows;
- number of input files;
- input file sizes;
- estimated row width;
- key frequencies;
- heavy hitter keys;
- number of distinct keys;
- target number of partitions;
- estimated partition sizes;
- max, mean, median, p95 partition size;
- variance and coefficient of variation;
- max/mean imbalance ratio.

Exact statistics are the default. In exact mode `key_frequencies` and
`distinct_keys` describe the full observed key distribution.

Approximate heavy hitter mode is a bounded summary mode. It may be used to
detect heavy keys without storing the full key frequency map, but its
`key_frequencies` field is only a truncated summary. Therefore:

- `distinct_keys` is unknown and must be serialized as `null`;
- `key_frequencies_exact` is `false`;
- `key_frequencies_truncated` is `true`;
- `normal_keys_materialized` is `false`.

The approximate summary must not be interpreted as the complete distribution of
normal keys.

## Heavy hitter detection

A key is considered heavy if its estimated frequency is significantly higher than the average frequency.

Basic rule:

heavy(key) = freq(key) > alpha * mean_freq

where alpha is configurable.

Alternative rule:

heavy(key) = freq(key) > L_rows

where L_rows is the maximum allowed number of rows per partition.

## Partition planning

The planner should:

1. Estimate the distribution of keys.
2. Detect heavy keys.
3. Assign normal keys by hash or complete load-aware placement.
4. Split heavy keys across several buckets.
5. Compute an output partition id for each record.
6. Estimate before/after skew and target-bound satisfaction.
7. Choose rewrite or no-op based on heavy keys, partition bounds, imbalance,
   and rewrite cost.
8. Minimize unnecessary rewriting and excessive partition count.

## Main strategy

Adaptive hash partitioning with selective salting:

- normal key: partition_id = hash(key) mod N;
- heavy key: partition_id = hash(key, salt) mod N;
- salt is deterministic and derived from row position, hash, or an additional stable column where available.

The number of salt buckets for a heavy key should be proportional to its frequency:

salt_count(key) = ceil(freq(key) / target_partition_rows)

If only an approximate heavy-key summary is available, load-aware placement of
normal keys is disabled because the planner does not know the full set of
normal keys. Normal rows then use hash fallback unless they belong to a planned
heavy key.

## Downstream Execution

Physical rewriting alone can help scan/filter workloads by changing file layout
and partition directories. It does not by itself rewrite Spark logical
operators.

For `group_by`, method-aware downstream execution uses two stages:

```text
partial groupBy: [_rp_partition_id, _rp_salt, key_columns...]
final groupBy:   [key_columns...]
```

If technical columns are disabled or missing, method-aware groupBy is degraded
or skipped by the Spark benchmark rather than silently claiming full salting.

For `join`, the planner can recommend:

- `broadcast_join`;
- `salted_heavy_key_join`;
- `heavy_key_isolation_join`;
- `physical_repartitioning`;
- `generic_join_repartitioning` when no join plan is available.

The Spark join benchmark implements method-aware join as an experimental
validation path. It supports single-column join keys and structured heavy-key
metadata. Composite join keys are not executed silently; the benchmark records
`skipped = true` with a skip reason.

## Storage-level output sizing

The implemented method is focused on adaptive partitioning with selective
salting. The writer also controls Parquet output file size by rolling
`part-xxxxx.parquet` files according to `storage.target_file_size_mb`; this is
an output-layer optimization, not a separate partitioning strategy.

`partitioning.strategy: "file_size_balancing"` is reserved for a future
standalone method that would explicitly split oversized input files and coalesce
small input files. In the current prototype the planner rejects this strategy
with a clear error and recommends `adaptive_hash_salt`.

Storage metadata such as `small_file_count`, `oversized_file_count`,
`target_file_size_mb`, and `min_file_size_mb` describes the file layer. These
fields should not be interpreted as evidence that standalone input file
coalescing/splitting has been implemented.

## Metadata

_partition_plan.json should contain:

- strategy name;
- key columns;
- target partition size;
- number of output partitions;
- heavy keys;
- salt count per heavy key;
- hash function;
- seed;
- creation timestamp;
- version.

_stats.json should contain:

- input statistics;
- skew statistics;
- before/after partition estimates.

_manifest.json should contain:

- output file list;
- partition ids;
- row counts per partition;
- file sizes where available.
