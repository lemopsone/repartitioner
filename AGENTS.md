# AGENTS.md

## Project

This repository implements a prototype for a diploma project:

"Method of adaptive data partitioning for preventing data skew in distributed big data processing systems."

The implemented software is an external preprocessing tool. It computes statistics over an input dataset, builds an adaptive partitioning plan, physically rewrites the dataset according to that plan, and produces Spark-compatible output data.

The core tool must not depend on Apache Spark. Spark is used only as a downstream consumer and as an integration/benchmarking environment.

## Main technical decisions

- Core language: Rust.
- Main output format: Apache Parquet.
- In-memory / columnar model: Apache Arrow where applicable.
- CLI configuration format: YAML or JSON.
- Metadata output: JSON.
- Experiment orchestration: Python.
- Downstream validation: Apache Spark, only in integration experiments.

## Conceptual model

Input:
- D: input dataset.
- C: parameters of the downstream computational job.
- R: resource constraints.
- K: partitioning key columns.
- L: upper threshold for partition size.
- N: target or maximum number of partitions.

Output:
- D': physically repartitioned dataset.
- P: adaptive partitioning plan.
- S: computed statistics.
- M: manifest of output files and partitions.

## Required output directory format

The tool should write a Spark-compatible dataset:

output_dataset/
- _partition_plan.json
- _stats.json
- _manifest.json
- ap_partition=0/
  - part-00000.parquet
- ap_partition=1/
  - part-00001.parquet
- ...

Do not claim that this creates internal Spark partitions or Spark bucketed tables. It creates a Spark-compatible prepartitioned Parquet dataset.

## Required modules

The Rust crate should be structured around these concerns:

- reader: read input datasets.
- statistics: compute dataset, key, and partition statistics.
- heavy_hitters: detect frequent keys.
- planner: build adaptive partitioning plan.
- partitioner: assign records to output partitions.
- writer: write Parquet output.
- manifest: write _manifest.json, _stats.json, _partition_plan.json.
- cli: parse command-line arguments and configuration.

## Method requirements

The method should compute or support the following statistics:

- total row count;
- input file sizes;
- key frequencies;
- heavy hitters;
- estimated partition sizes;
- max partition size;
- mean partition size;
- median or approximate median;
- p95 partition size;
- variance of partition sizes;
- coefficient of variation;
- max/mean imbalance ratio.

The main strategy is adaptive hash partitioning with selective splitting/salting of heavy keys. Optional future strategies may include quantile/range partitioning and file-size balancing.

## Testing requirements

Add tests for:

- preserving row count after partitioning;
- preserving key values after partitioning;
- detecting heavy hitters on synthetic skewed data;
- not overreacting to uniform data;
- generating valid JSON metadata;
- producing deterministic output when a seed is fixed;
- reducing max/mean partition imbalance on skewed data.

## Experiments

Python scripts may be used to generate synthetic data and run experiments.

Required scenarios:
- uniform key distribution;
- one heavy key;
- multiple heavy keys;
- Zipf distribution;
- group-by oriented dataset;
- join-oriented dataset.

Measure:
- preprocessing time;
- statistics computation time;
- partition planning time;
- output writing time;
- peak memory where possible;
- partition imbalance before and after preprocessing;
- Spark pipeline time before and after preprocessing;
- end-to-end time including preprocessing.

## Constraints

- Do not implement the core method as PySpark code.
- Do not introduce Spark as a dependency of the Rust core.
- Do not modify Spark internals.
- Do not assume preprocessing always improves end-to-end time for one-off jobs.
- Always separate preprocessing cost from downstream Spark pipeline cost.
- Prefer clear, testable code over premature optimization.
- Keep generated code documented enough for a diploma prototype.