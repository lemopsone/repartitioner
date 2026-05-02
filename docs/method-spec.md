# Method specification

## Goal

Build an adaptive partitioning plan from computed statistics and use it to rewrite the input dataset into a more balanced physical layout.

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

- Repartitioned dataset D'.
- Partitioning plan P.
- Statistics file S.
- Manifest M.

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
3. Assign normal keys by hash partitioning.
4. Split heavy keys across several buckets.
5. Compute an output partition id for each record.
6. Try to keep partition sizes below L.
7. Minimize unnecessary rewriting and excessive partition count.

## Main strategy

Adaptive hash partitioning with selective salting:

- normal key: partition_id = hash(key) mod N;
- heavy key: partition_id = hash(key, salt) mod N;
- salt is deterministic and derived from row position, hash, or an additional stable column where available.

The number of salt buckets for a heavy key should be proportional to its frequency:

salt_count(key) = ceil(freq(key) / target_partition_rows)

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