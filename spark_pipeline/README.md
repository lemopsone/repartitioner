# Spark Benchmark

Скрипты для прогона тестовых spark-pipeline'ов с целью исследования эффективности работы
разрабатываемого метода.

Установка зависимостей:

```bash
pip install -r spark_pipeline/requirements.txt
```

Два прогона (JOIN + groupBy):

```bash
python3 spark_pipeline/benchmark.py \
  --original data/heavy.parquet \
  --preprocessed data/heavy_partitioned \
  --json-report reports/spark-heavy.json \
  --csv-report reports/spark-heavy.csv \
  --shuffle-partitions 16
```

Для `group_by` отчёт содержит три режима:

- `baseline`: `original.groupBy(key_column).count()`.
- `physical_only`: `preprocessed.groupBy(key_column).count()`.
- `method_aware`: двухстадийная агрегация по технической колонке партиции и ключу.

`method_aware` включается автоматически, если в preprocessed dataset есть
`_partition_plan.json`. Его также можно запросить явно:

```bash
python3 spark_pipeline/run_groupby.py \
  --original data/heavy.parquet \
  --preprocessed data/heavy_partitioned \
  --json-report reports/groupby-heavy.json \
  --csv-report reports/groupby-heavy.csv \
  --include-method-aware
```

Для method-aware groupBy колонка партиции берётся из
`technical_columns.partition_column` в `_partition_plan.json`. Если техническая
колонка не записана, benchmark пробует Hive-style колонки `rp_partition` и
`ap_partition`, если Spark видит их при чтении dataset.

В JSON/CSV отчётах поле `mode` разделяет `baseline`, `physical_only` и
`method_aware`, а `correctness` показывает совпадение числа строк и числа групп
с baseline.

Только groupBy:

```bash
python3 spark_pipeline/run_groupby.py \
  --original data/heavy.parquet \
  --preprocessed data/heavy_partitioned \
  --json-report reports/groupby-heavy.json \
  --csv-report reports/groupby-heavy.csv
```

Только JOIN:

```bash
python3 spark_pipeline/run_join.py \
  --original data/heavy.parquet \
  --preprocessed data/heavy_partitioned \
  --join-right data/dimension.parquet \
  --json-report reports/join-heavy.json \
  --csv-report reports/join-heavy.csv
```

Если не указывать флаг `--join-right`, скрипт собирает правую часть из оригинального датасета
до начала замеров времени. Эти действия не попадают в генерируемые отчеты.
