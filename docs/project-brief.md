# Project brief

## Topic

Method of adaptive data partitioning for preventing data skew in distributed big data processing systems.

## Research background

Data skew occurs when the amount of data or computational load assigned to one or more partitions significantly exceeds the load of other partitions. In distributed systems, this creates straggler tasks and increases total job execution time.

The analytical part of the diploma work formalizes the skew mitigation problem as finding a new partitioning scheme P' such that:

- the maximum partition size is bounded by L;
- the variance of partition sizes is minimized;
- the overhead of transforming the old partitioning scheme into the new one is minimized.

## Existing method groups

The analytical section distinguishes these groups of methods:

- preprocessing and key modification;
- partitioning scheme modification;
- join optimization;
- computational load redistribution;
- storage-level methods.

The developed method belongs primarily to the data-level preprocessing / partitioning scheme modification group.

## Diploma task requirements

The diploma assignment requires:

- developing an adaptive data partitioning method;
- defining input statistics used for skew detection;
- describing key stages of the method;
- justifying technologies and software tools;
- describing software architecture;
- implementing a prototype;
- testing the developed software;
- evaluating method efficiency under different data distribution scenarios.

## Implementation direction

The software is an external preprocessing tool. It receives an input dataset, computes statistics, builds an adaptive partitioning plan, writes a physically repartitioned dataset, and emits metadata files. Apache Spark is used only as a downstream processing engine for integration testing and performance evaluation.