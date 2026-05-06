# Набор скриптов для проведения исследования

Для использования необходимо установить зависимости

```bash
pip install pyarrow
# или
pip install -r experiments/requirements.txt
```

Генерация датасетов:

```bash
python3 experiments/generate_uniform.py --output data/uniform.parquet --rows 100000
python3 experiments/generate_heavy_key.py --output data/heavy.parquet --rows 100000 --heavy-fraction 0.5
python3 experiments/generate_multi_heavy_key.py --output data/multi-heavy.parquet --rows 100000 --heavy-keys 4
python3 experiments/generate_zipf.py --output data/zipf.parquet --rows 100000 --zipf-exponent 1.2
```

Запуск core-метода через Python-обертку с генерацией отчета:

```bash
python3 experiments/run_preprocessor.py \
  --input data/heavy.parquet \
  --output data/heavy_partitioned \
  --result reports/heavy-result.json \
  --max-partitions 16
```

Сборка метаданных и результатов с полученного перераспределения:

```bash
python3 experiments/collect_results.py \
  --output-dataset data/heavy_partitioned \
  --input-metadata data/heavy.parquet.json \
  --result reports/heavy-result.json
```

## Полный набор экспериментов

Единый runner для раздела исследования:

```bash
.venv/bin/python experiments/run_suite.py \
  --run-dir experiments/runs/main-suite \
  --release
```

По умолчанию запускается матрица:

- distributions: `uniform`, `single_heavy`, `multi_heavy`, `zipf`;
- rows: `10000`, `100000`, `1000000`;
- heavy_fraction: `0.1`, `0.25`, `0.5`, `0.75`;
- max_partitions: `4`, `8`, `16`, `32`;
- target_partition_size_mb: `16`, `64`, `128`.

Для быстрой проверки runner-а:

```bash
.venv/bin/python experiments/run_suite.py \
  --run-dir /tmp/repartitioner-suite-smoke \
  --rows 10000 \
  --distributions single_heavy \
  --heavy-fractions 0.5 \
  --max-partitions 4 \
  --target-partition-size-mb 16 \
  --limit 1
```

Если Spark в окружении временно недоступен, можно проверить только генерацию и
preprocessor:

```bash
.venv/bin/python experiments/run_suite.py \
  --run-dir /tmp/repartitioner-suite-no-spark \
  --rows 10000 \
  --limit 2 \
  --skip-spark
```

Runner сохраняет для каждого сценария:

- сгенерированный dataset и `input.parquet.json`;
- YAML-конфиг preprocessor-а;
- JSON-результат preprocessor-а;
- Spark JSON/CSV benchmark result.

Итоговые файлы:

- `summary.csv` — таблица для графиков ВКР;
- `summary.json` — те же результаты плюс список неуспешных сценариев.

В `summary.csv` отдельно фиксируются Spark-only времена и end-to-end времена:

- `spark_baseline_seconds`;
- `spark_physical_only_seconds`;
- `spark_method_aware_seconds`;
- `end_to_end_physical_only_seconds`;
- `end_to_end_method_aware_seconds`.

Для no-op сценариев method-aware Spark-режим может быть `null`, потому что
preprocessor не материализует технические колонки и Spark читает исходный input
как reused dataset.
