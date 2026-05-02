# Experiment Scripts

Install Python dependency:

```bash
pip install pyarrow
```

Generate datasets:

```bash
python3 experiments/generate_uniform.py --output data/uniform.parquet --rows 100000
python3 experiments/generate_heavy_key.py --output data/heavy.parquet --rows 100000 --heavy-fraction 0.5
python3 experiments/generate_multi_heavy_key.py --output data/multi-heavy.parquet --rows 100000 --heavy-keys 4
python3 experiments/generate_zipf.py --output data/zipf.parquet --rows 100000 --zipf-exponent 1.2
```

Run the Rust preprocessor and collect one result JSON:

```bash
python3 experiments/run_preprocessor.py \
  --input data/heavy.parquet \
  --output data/heavy_partitioned \
  --result reports/heavy-result.json \
  --max-partitions 16
```

Collect results again from an existing output dataset:

```bash
python3 experiments/collect_results.py \
  --output-dataset data/heavy_partitioned \
  --input-metadata data/heavy.parquet.json \
  --result reports/heavy-result.json
```
