# Spark Benchmark Scripts

These scripts are only for downstream integration and benchmarking. They are not used by the Rust core.

Install Spark Python dependencies in your experiment environment:

```bash
pip install -r spark_pipeline/requirements.txt
```

Run both workloads:

```bash
python3 spark_pipeline/benchmark.py \
  --original data/heavy.parquet \
  --preprocessed data/heavy_partitioned \
  --json-report reports/spark-heavy.json \
  --csv-report reports/spark-heavy.csv \
  --shuffle-partitions 16
```

Run only groupBy:

```bash
python3 spark_pipeline/run_groupby.py \
  --original data/heavy.parquet \
  --preprocessed data/heavy_partitioned \
  --json-report reports/groupby-heavy.json \
  --csv-report reports/groupby-heavy.csv
```

Run only join:

```bash
python3 spark_pipeline/run_join.py \
  --original data/heavy.parquet \
  --preprocessed data/heavy_partitioned \
  --join-right data/dimension.parquet \
  --json-report reports/join-heavy.json \
  --csv-report reports/join-heavy.csv
```

If `--join-right` is omitted, the script builds a cached right side from distinct keys in the original dataset before timed join runs. Reports include downstream Spark time only for the timed groupBy/join action.
