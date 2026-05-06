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

Запуск Rust-ядра через Python-обертку с генерацией отчета:

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

Единый сценарный runner для раздела исследования:

```bash
.venv/bin/python experiments/run_suite.py \
  --run-dir experiments/runs/main-suite \
  --release
```

По умолчанию запускается матрица:

- распределения: `uniform_no_skew`, `uniform`, `single_heavy`,
  `multi_heavy`, `zipf`;
- количество строк: `10000`, `100000`, `1000000`;
- доля heavy key: `0.1`, `0.25`, `0.5`, `0.75`;
- максимум партиций: `4`, `8`, `16`, `32`;
- целевой размер партиции: `16`, `64`, `128` МБ.

Для быстрой проверки runner-а:

```bash
.venv/bin/python experiments/run_suite.py \
  --run-dir /tmp/repartitioner-suite-smoke \
  --rows 10000 \
  --distributions uniform_no_skew,single_heavy \
  --heavy-fractions 0.5 \
  --max-partitions 4 \
  --target-partition-size-mb 16 \
  --limit 2
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

Сценарный runner сохраняет для каждого сценария:

- сгенерированный dataset и `input.parquet.json`;
- YAML-конфиг preprocessor-а;
- JSON-результат preprocessor-а;
- JSON/CSV-отчёт Spark benchmark-а.

Итоговые файлы:

- `summary.csv` — таблица для графиков ВКР;
- `summary.json` — те же результаты плюс список неуспешных сценариев.

В `summary.csv` отдельно фиксируются времена только Spark и end-to-end времена:

- `before_max_mean_ratio`;
- `after_max_mean_ratio`;
- `skew_reduction_ratio`;
- `spark_baseline_seconds`;
- `spark_physical_only_seconds`;
- `spark_method_aware_seconds`;
- `end_to_end_physical_only_seconds`;
- `end_to_end_method_aware_seconds`.

Дополнительно сохраняются:

- `before_max_partition_size`;
- `after_max_partition_size`;
- `before_cv`;
- `after_cv`;
- `target_rows_satisfied_after`;
- `heavy_hitter_count`;
- `output_partitions`;
- `output_file_count`;
- `partitioning_strategy`.

Для no-op сценариев method-aware Spark-режим может быть `null`, потому что
preprocessor не материализует технические колонки и Spark читает исходный input
как reused dataset.

Сценарий `uniform_no_skew` нужен как контрольный пример: если данные уже
распределены равномерно, heavy hitters отсутствуют и ограничение `L` выполняется,
метод не обязан физически переписывать dataset. В таком запуске ожидается
`rewrite_required=false`, `action=no_op`, а `preprocessing_writing_seconds`
отражает только запись metadata и должен быть близок к нулю.

## Границы интерпретации

Набор экспериментов оценивает прототип, а не произвольную Spark-интеграцию:

- Rust-ядро не зависит от Spark.
- Parquet используется как основной формат прототипа.
- CSV-ввод можно использовать для демонстрации слоя адаптеров, но CSV-вывод
  отсутствует.
- `file_size_balancing` как отдельная стратегия не участвует в наборе
  экспериментов; в отчётах отражается только rolling выходных файлов на уровне
  writer-а.
- Method-aware join является экспериментальным режимом Spark benchmark-а и может
  быть пропущен с `skip_reason`, если metadata или схема не позволяют выполнить
  его безопасно.
